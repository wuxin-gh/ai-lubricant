# Keychain SOP

## Purpose

Keep API keys, passwords, cookies and other sensitive credentials out of `memory/`, SOP files, prompts and ordinary workspace text.

## Storage

Use the operating system keyring (`keyring` / platform credential store) or the platform's encrypted credential storage. Never write plaintext secrets to `memory/global_mem.txt`, `memory/sop/`, `workspace/` logs or an L1 pointer.

## Access

When the Agent needs a credential, use a controlled capability or approved runtime binding that returns only the minimum value needed for the current operation. Do not print it in tool results, checkpoints or final responses.

## Rotation and audit

Rotate credentials through the platform's credential-management flow. Record only non-sensitive metadata (credential name, scope, rotation date) as a verified fact. If a secret appears in output, stop, redact it and ask the user whether it must be rotated.

## Limitations

This SOP documents policy; it does not add a keychain tool. Do not use `code_run` to inspect arbitrary credential databases.
