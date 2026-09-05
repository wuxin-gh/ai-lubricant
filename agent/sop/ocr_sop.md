# OCR SOP

## Purpose

Extract text from images when reading a web page or working file requires it.

## How

OCR is available through `code_run`. The platform standard is `rapidocr_onnxruntime`.

1. Save or download the image into the workspace (e.g. `workspace/ocr/input.png`).
2. Call `code_run` with a short Python script that uses `rapidocr_onnxruntime.RapidOCR`:
   ```python
   from rapidocr_onnxruntime import RapidOCR
   engine = RapidOCR()
   result, _ = engine("workspace/ocr/input.png")
   print(result)
   ```
3. Inspect the extracted text; confirm it is coherent before acting on it.
4. After a verified reusable extraction pipeline, call `start_long_term_update` with the script steps.

## Rules

- OCR runs under the same code_run approval rules; never bypass approval.
- Do not store extracted secrets in long-term memory; store only the verified reusable procedure.
- If the OCR backend is not installed, ask the user to provision it rather than silently skipping.

## Future

A future `capability_call` capability may expose OCR as a service; until then, use `code_run` as documented here.
