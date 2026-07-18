# ChatGPT Web Protocol References

This implementation was independently rewritten in Go with the following MIT-licensed references:

- `yukkcat/chatgpt2api` at commit `2d8009e4c9a768f9d1af1945eeb2ce65456c5df3`:
  primary reference for the ChatGPT Web conversation, streaming, image, and model-catalog behavior.
- `basketikun/chatgpt2api` at commit `1f96b49b2bf35e607a2c587e3fb9d40c5acb475d`:
  reference for password OAuth, refresh-token rotation, browser session headers, and Sentinel request flow.

The locked revisions publish identical copyright and MIT license text. `LICENSE`
retains the notice from `yukkcat/chatgpt2api`, and `LICENSE.basketikun` retains
the notice from `basketikun/chatgpt2api`. No Python runtime or source file is
embedded in the binary.
