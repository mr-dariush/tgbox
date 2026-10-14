package syncstate

import (
	"context"
	"fmt"
	"sync"

	"github.com/gotd/td/tg"
	"golang.org/x/sync/singleflight"
)

// DifferenceAPI abstracts the MTProto RPC method for difference retrieval.
type DifferenceAPI interface {
	UpdatesGetDifference(ctx context.Context, request *tg.UpdatesGetDifferenceRequest) (tg.UpdatesDifferenceClass, error)
}

// DifferenceBatch represents an atomic set of messages and updates retrieved during a difference sync.
type DifferenceBatch struct {
	Messages          []tg.MessageClass
	EncryptedMessages []tg.EncryptedMessageClass
	OtherUpdates      []tg.UpdateClass
	Users             []tg.UserClass
	Chats             []tg.ChatClass
	IsIntermediate    bool
}

// BatchHandler defines the consumer callback signature for processing retrieved difference slices.
type BatchHandler func(batch DifferenceBatch) error

// DifferenceFetcher coordinates sequential, pull-based getDifference pagination with O(1) streaming checkpoints.
type DifferenceFetcher struct {
	api       DifferenceAPI
	vector    *StateVector
	flusher   *CheckpointFlusher
	gapBuffer *GapBuffer
	onTooLong func()
	sfg       singleflight.Group
	mu        sync.Mutex
}

// NewDifferenceFetcher constructs a DifferenceFetcher bound to state registers and flusher.
func NewDifferenceFetcher(
	api DifferenceAPI,
	vector *StateVector,
	flusher *CheckpointFlusher,
	gapBuffer *GapBuffer,
	onTooLong func(),
) *DifferenceFetcher {
	return &DifferenceFetcher{
		api:       api,
		vector:    vector,
		flusher:   flusher,
		gapBuffer: gapBuffer,
		onTooLong: onTooLong,
	}
}

// FetchDifference executes paginated difference recovery at the consumer's processing pace.
// Concurrent calls are strictly coalesced via singleflight to prevent server request flooding.
func (f *DifferenceFetcher) FetchDifference(ctx context.Context, handler BatchHandler) error {
	const singleflightKey = "syncstate.getDifference"
	_, err, _ := f.sfg.Do(singleflightKey, func() (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		return nil, f.runFetchLoop(ctx, handler)
	})
	return err
}

func (f *DifferenceFetcher) runFetchLoop(ctx context.Context, handler BatchHandler) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		snap := f.vector.Snapshot()
		req := &tg.UpdatesGetDifferenceRequest{
			Pts:  int(snap.PTS),
			Qts:  int(snap.QTS),
			Date: int(snap.Date),
		}

		res, err := f.api.UpdatesGetDifference(ctx, req)
		if err != nil {
			return fmt.Errorf("syncstate: getDifference failed: %w", err)
		}

		shouldContinue, err := f.handlePolymorphicResult(ctx, res, handler)
		if err != nil {
			return err
		}
		if !shouldContinue {
			return nil
		}
	}
}

// handlePolymorphicResult routes polymorphic responses of updates.getDifference.
func (f *DifferenceFetcher) handlePolymorphicResult(
	ctx context.Context,
	res tg.UpdatesDifferenceClass,
	handler BatchHandler,
) (bool, error) {
	switch d := res.(type) {
	case *tg.UpdatesDifferenceEmpty:
		f.applyEmptyDifference(d)
		return false, nil

	case *tg.UpdatesDifferenceSlice:
		return true, f.commitSliceDifference(ctx, d, handler)

	case *tg.UpdatesDifference:
		return false, f.commitFinalDifference(ctx, d, handler)

	case *tg.UpdatesDifferenceTooLong:
		f.handleTooLong(ctx, d.Pts)
		return false, nil

	default:
		return false, fmt.Errorf("syncstate: unexpected difference type: %T", res)
	}
}

func (f *DifferenceFetcher) applyEmptyDifference(d *tg.UpdatesDifferenceEmpty) {
	f.vector.SetDate(int32(d.Date))
	f.vector.SetSeq(int32(d.Seq))
}

// commitSliceDifference dispatches an intermediate difference slice and persists the checkpoint.
func (f *DifferenceFetcher) commitSliceDifference(
	ctx context.Context,
	d *tg.UpdatesDifferenceSlice,
	handler BatchHandler,
) error {
	batch := DifferenceBatch{
		Messages:          d.NewMessages,
		EncryptedMessages: d.NewEncryptedMessages,
		OtherUpdates:      d.OtherUpdates,
		Users:             d.Users,
		Chats:             d.Chats,
		IsIntermediate:    true,
	}

	if handler != nil {
		if err := handler(batch); err != nil {
			return err
		}
	}

	f.applyServerState(d.IntermediateState)

	if f.flusher != nil {
		if err := f.flusher.Flush(ctx); err != nil {
			return fmt.Errorf("syncstate: flush intermediate checkpoint: %w", err)
		}
	}

	return nil
}

func (f *DifferenceFetcher) commitFinalDifference(
	ctx context.Context,
	d *tg.UpdatesDifference,
	handler BatchHandler,
) error {
	batch := DifferenceBatch{
		Messages:          d.NewMessages,
		EncryptedMessages: d.NewEncryptedMessages,
		OtherUpdates:      d.OtherUpdates,
		Users:             d.Users,
		Chats:             d.Chats,
		IsIntermediate:    false,
	}

	// 1. Dispatch batch to handlers
	if handler != nil {
		if err := handler(batch); err != nil {
			return err
		}
	}

	// 2. Advance local state vector to Final State
	f.applyServerState(d.State)

	// 3. Commit final transaction to disk
	if f.flusher != nil {
		if err := f.flusher.Flush(ctx); err != nil {
			return fmt.Errorf("syncstate: flush persistent checkpoint: %w", err)
		}
	}

	return nil
}

func (f *DifferenceFetcher) handleTooLong(ctx context.Context, remotePTS int) {
	f.vector.SetPTS(int32(remotePTS))
	if f.gapBuffer != nil {
		f.gapBuffer.Clear()
	}
	if f.flusher != nil {
		_ = f.flusher.Flush(ctx)
	}
	if f.onTooLong != nil {
		f.onTooLong()
	}
}

func (f *DifferenceFetcher) applyServerState(s tg.UpdatesState) {
	f.vector.SetPTS(int32(s.Pts))
	f.vector.SetQTS(int32(s.Qts))
	f.vector.SetDate(int32(s.Date))
	f.vector.SetSeq(int32(s.Seq))
}
