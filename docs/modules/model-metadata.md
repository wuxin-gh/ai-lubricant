# 模型元数据

对应文件：`model_metadata.py`、`model_metadata.json`

## 职责

模型元数据用于补齐 Provider 返回模型列表中缺失的公共字段。

默认字段：

- `max_tokens`
- `max_context_tokens`
- `input_modalities`
- `output_modalities`
- `function_calling`
- `auto_search`
- `auto_thinking`
- `capabilities`

## 工作方式

`apply_model_metadata(provider, model)`：

1. 根据 model ID 读取 `model_metadata.json`。
2. 先合并 fallback default。
3. 再合并配置中的 default。
4. 最后合并具体模型配置。
5. Provider 已返回的字段优先。
6. 如果有 `input_modalities`，补 `multimodal`。

