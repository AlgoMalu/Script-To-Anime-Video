import os
from openai import OpenAI

client = OpenAI(
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

resp = client.chat.completions.create(
    model="qwen3.5-plus",  # 例如 qwen-plus
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "只回复：API通了"}
    ],
    temperature=0
)

print(resp.choices[0].message.content)