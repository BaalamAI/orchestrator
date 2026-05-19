package llm

import (
	"context"
	"errors"

	"github.com/baalamai/orchestrator"
)

// MultimodalClient is the outbound port for completion calls with file input
// (PDFs, images, audio, video). It is separate from Client and StructuredClient
// because providers diverge on file handling:
//
//   - Providers with a Files API (Gemini) support upload → reference → delete
//     for cheap reuse across multiple calls on the same document.
//   - Providers without a Files API (Anthropic) inline files as base64 on every
//     call; their MultimodalClient.UploadFromURL / GenerateFromRef / DeleteFile
//     implementations return ErrFilesAPIUnsupported.
//
// Adapters that support both Client and MultimodalClient typically expose them
// as two distinct constructors backed by a shared underlying SDK client
// (e.g. NewClient / NewMultimodalClient in lib/llm-gemini).
type MultimodalClient interface {
	// GenerateFromURL fetches the file from URL and sends it inline with the
	// prompt in a single call. One-shot — no prior upload required. Use this
	// when the file is consumed once.
	GenerateFromURL(ctx context.Context, req MultimodalURLRequest) (*MultimodalResponse, error)

	// UploadFromURL downloads the file from URL and uploads it to the
	// provider's Files API. Returns a reusable file URI for GenerateFromRef.
	// Returns ErrFilesAPIUnsupported when the provider has no Files API.
	UploadFromURL(ctx context.Context, req MultimodalUploadRequest) (string, error)

	// GenerateFromRef sends a prompt against a previously uploaded file URI.
	// Cheaper than GenerateFromURL when the same document is queried
	// repeatedly. Returns ErrFilesAPIUnsupported when the provider has no
	// Files API.
	GenerateFromRef(ctx context.Context, req MultimodalRefRequest) (*MultimodalResponse, error)

	// DeleteFile removes a previously uploaded file by URI. Returns
	// ErrFilesAPIUnsupported when the provider has no Files API.
	DeleteFile(ctx context.Context, fileURI string) error
}

// ErrFilesAPIUnsupported is returned by MultimodalClient methods that require
// a provider-side Files API when the provider does not expose one.
var ErrFilesAPIUnsupported = errors.New("llm: provider does not support a Files API")

// MultimodalURLRequest is the input for one-shot multimodal generation from a
// remote file URL. The adapter is responsible for downloading the file and
// inlining it in the request (subject to provider size limits, typically
// ~20–50 MB).
type MultimodalURLRequest struct {
	// Model is the provider-specific model identifier.
	Model string
	// URL is the file URL the adapter will download.
	URL string
	// MIMEType is the file's content type (e.g. "application/pdf",
	// "image/png"). Required — adapters do not infer it from URL extensions.
	MIMEType string
	// Prompt is the text portion of the multimodal prompt.
	Prompt string
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps the completion length; 0 means provider default.
	MaxTokens int32
}

// MultimodalUploadRequest is the input for Files-API uploads. The adapter
// downloads the file from URL and uploads it to the provider.
type MultimodalUploadRequest struct {
	// URL is the file URL the adapter will download and re-upload.
	URL string
	// MIMEType is the file's content type. Required.
	MIMEType string
}

// MultimodalRefRequest is the input for generation against a previously
// uploaded file URI (only valid for providers with a Files API).
type MultimodalRefRequest struct {
	// Model is the provider-specific model identifier.
	Model string
	// FileURI is the URI returned by a prior UploadFromURL call.
	FileURI string
	// MIMEType is the file's content type. Required.
	MIMEType string
	// Prompt is the text portion of the multimodal prompt.
	Prompt string
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps the completion length; 0 means provider default.
	MaxTokens int32
}

// MultimodalResponse is the canonical multimodal-completion response.
type MultimodalResponse struct {
	// Content is the model's response text.
	Content string
	// Usage is the token usage reported by the provider (may be nil if
	// unsupported).
	Usage *orchestrator.Usage
}
