package interceptor

import (
	"context"
	"fmt"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type contextKey string

const (
	isOutgoingKey   contextKey = "tgbox_outgoing"
	outgoingPeerKey contextKey = "tgbox_outgoing_peer"
)

// WithOutgoing marks the context as originating from an outgoing (self-sent) call.
func WithOutgoing(ctx context.Context) context.Context {
	return context.WithValue(ctx, isOutgoingKey, true)
}

// IsOutgoing checks if the context originates from an outgoing (self-sent) call.
func IsOutgoing(ctx context.Context) bool {
	return ctx.Value(isOutgoingKey) != nil
}

// WithOutgoingPeer associates the destination input peer with an outgoing request context.
func WithOutgoingPeer(ctx context.Context, peer tg.InputPeerClass) context.Context {
	return context.WithValue(ctx, outgoingPeerKey, peer)
}

// OutgoingPeer retrieves the target input peer from the context if present.
func OutgoingPeer(ctx context.Context) tg.InputPeerClass {
	if val, ok := ctx.Value(outgoingPeerKey).(tg.InputPeerClass); ok {
		return val
	}
	return nil
}

// DispatcherFunc defines the callback signature to inject synthetic updates back into the pipeline.
type DispatcherFunc func(context.Context, tg.UpdatesClass) error

// TelemetryExtractor defines a callback to extract trace and client identifiers without causing import cycles.
type TelemetryExtractor func(context.Context) (traceID string, clientID int64)

// OutgoingInterceptor wraps a tg.Invoker to intercept outgoing message/media methods,
// emit synthetic UpdatesClass payloads, and trace network latencies via OpenTelemetry.
type OutgoingInterceptor struct {
	next      tg.Invoker
	dispatch  DispatcherFunc
	extractor TelemetryExtractor
	tracer    trace.Tracer
}

// NewOutgoingInterceptor creates a new decorator over a base tg.Invoker with telemetry support.
func NewOutgoingInterceptor(next tg.Invoker, dispatch DispatcherFunc, extractor TelemetryExtractor) *OutgoingInterceptor {
	return &OutgoingInterceptor{
		next:      next,
		dispatch:  dispatch,
		extractor: extractor,
		tracer:    otel.Tracer("tgbox/interceptor"),
	}
}

// Invoke executes the MTProto RPC call, tracing its execution time and sniffing outgoing message payloads.
func (o *OutgoingInterceptor) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	// Dynamically extract the MTProto RPC method name for the trace span
	methodName := fmt.Sprintf("%T", input)

	// Start a network client span
	ctx, span := o.tracer.Start(ctx, methodName, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	// Inject SDK-specific metadata if available
	if o.extractor != nil {
		traceID, clientID := o.extractor(ctx)
		if traceID != "" {
			span.SetAttributes(attribute.String("tgbox.trace_id", traceID))
		}
		if clientID != 0 {
			span.SetAttributes(attribute.Int64("tgbox.client_id", clientID))
		}
	}

	err := o.next.Invoke(ctx, input, output)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err //nolint:wrapcheck // transparent invoker decorator; callers match on the original tgerr
	}

	span.SetStatus(codes.Ok, "success")

	// Sniff only if the call succeeded and matches an outgoing message/media method
	var targetPeer tg.InputPeerClass
	switch req := input.(type) {
	case *tg.MessagesSendMessageRequest:
		targetPeer = req.Peer
	case *tg.MessagesSendMediaRequest:
		targetPeer = req.Peer
	case *tg.MessagesSendMultiMediaRequest:
		targetPeer = req.Peer
	case *tg.MessagesForwardMessagesRequest:
		targetPeer = req.ToPeer
	default:
		return nil
	}

	if updates, ok := output.(tg.UpdatesClass); ok {
		outCtx := WithOutgoing(ctx)
		if targetPeer != nil {
			outCtx = WithOutgoingPeer(outCtx, targetPeer)
		}
		_ = o.dispatch(outCtx, updates)
	}

	return nil
}
