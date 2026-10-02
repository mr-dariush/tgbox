package router_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/router"
)

// TestNode_Match tests pattern matching and parameter extraction in the router tree.
func TestNode_Match(t *testing.T) {
	type testCase struct {
		name        string
		routes      map[string]string // pattern -> handler identifier
		searchPath  string
		wantHandler string
		wantParams  map[string]string
		wantFound   bool
	}

	cases := []testCase{
		{
			name: "Static Match",
			routes: map[string]string{
				"/start": "start_handler",
				"/help":  "help_handler",
			},
			searchPath:  "/start",
			wantHandler: "start_handler",
			wantParams:  map[string]string{},
			wantFound:   true,
		},
		{
			name: "Single Parameter Match",
			routes: map[string]string{
				"/user_{id}": "user_handler",
			},
			searchPath:  "/user_12345",
			wantHandler: "user_handler",
			wantParams:  map[string]string{"id": "12345"},
			wantFound:   true,
		},
		{
			name: "Multi Parameter Match",
			routes: map[string]string{
				"/ban_{user_id}_{duration}": "ban_handler",
			},
			searchPath:  "/ban_12345_24h",
			wantHandler: "ban_handler",
			wantParams:  map[string]string{"user_id": "12345", "duration": "24h"},
			wantFound:   true,
		},
		{
			name: "No Match Fallback",
			routes: map[string]string{
				"/start": "start_handler",
			},
			searchPath:  "/non_existent",
			wantHandler: "",
			wantParams:  nil,
			wantFound:   false,
		},
		{
			name: "Backtracking with Dynamic Delimiter",
			routes: map[string]string{
				"/ban_{user_id}":            "ban_simple_handler",
				"/ban_{user_id}_{duration}": "ban_detailed_handler",
			},
			searchPath:  "/ban_12345_24h",
			wantHandler: "ban_detailed_handler",
			wantParams:  map[string]string{"user_id": "12345", "duration": "24h"},
			wantFound:   true,
		},
		{
			name: "Static Command with Raw Suffix Arguments",
			routes: map[string]string{
				"/start": "start_handler",
			},
			searchPath:  "/start payload_value_123",
			wantHandler: "start_handler",
			wantParams:  map[string]string{"args": "payload_value_123"},
			wantFound:   true,
		},
		{
			name: "Static Command with Suffix and Redundant Whitespaces",
			routes: map[string]string{
				"/start": "start_handler",
			},
			searchPath:  "/start   multiple   trailing   arguments   ",
			wantHandler: "start_handler",
			wantParams:  map[string]string{"args": "multiple   trailing   arguments"},
			wantFound:   true,
		},
		{
			name: "Command Prefix Safety (No false positives)",
			routes: map[string]string{
				"/start": "start_handler",
			},
			searchPath:  "/start_payload",
			wantHandler: "",
			wantParams:  nil,
			wantFound:   false,
		},
		{
			name: "Parametric Route Priority Over Generic Suffix",
			routes: map[string]string{
				"/start":           "start_handler",
				"/start {payload}": "start_parametric_handler",
			},
			searchPath:  "/start my_explicit_payload",
			wantHandler: "start_parametric_handler",
			wantParams:  map[string]string{"payload": "my_explicit_payload"},
			wantFound:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := router.NewNode[string]()
			for pattern, handler := range tc.routes {
				tree.Insert(pattern, handler)
			}

			handler, params, found := tree.Match(tc.searchPath)
			require.Equal(t, tc.wantFound, found)
			if tc.wantFound {
				assert.Equal(t, tc.wantHandler, handler)
				assert.Equal(t, tc.wantParams, params)
			} else {
				assert.Nil(t, params)
			}
		})
	}
}
