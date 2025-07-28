from openai import AzureOpenAI
import extractteaminfo
import json

# test


with open('/Users/sethrojas/documents/codeprojects/baresearch/TeamWebsiteHTMLs/2021-2022/Alabama.html', 'r') as file:
    html_content = file.read()

csvstr = extractteaminfo.get_info_from_htmlcontent(html_content)

deployment = "gpt-4-32k"
with open('OldPlayerInformationExtraction/apikeys.json', 'r') as file:
    data = json.load(file)['Azure']
    subscription_key = data['key']
    endpoint = data['endpoint']


# Initialize Azure OpenAI client with key-based authentication    
client = AzureOpenAI(  
    azure_endpoint=endpoint,  
    api_key=subscription_key,  
    api_version="2024-05-01-preview",  
)

prompt = """ 
Retrieve player information from the following text and return it in CSV format with these headers everytime: Player, Position, Height, Weight, Year, Homestate. Ensure the following:

- Homestate: Leave blank for international players, use state abbreviation (e.g., IL for Illinois).
- Height: Format as feet-inches (e.g., 6-2).
- Position: Use G (Guard), F (Forward), C (Center).
- Weight: Only the number in pounds (e.g., 180).
- Year: Use Fr, So, Jr, Sr, Gr, or R- for redshirt.
- If missing, leave the field blank (e.g., "").
* Do not include non-player info or comments.
* Only return in CSV format, no extra comments or notes.

Here is the text:


"""

content = prompt + csvstr
# content = 'Capital of France?'

#Prepare the chat prompt 
chat_prompt = [
    {
        "role": "system",
        "content": [
            {
                "type": "text",
                "text": content
            }
        ]
    }
] 
    
# Include speech result if speech is enabled  
messages = chat_prompt  
    
# Generate the completion  
completion = client.chat.completions.create(  
    model=deployment,  
    messages=messages,  
    max_tokens=800,  
    temperature=0.7,  
    top_p=0.95,  
    frequency_penalty=0,  
    presence_penalty=0,  
    stop=None,  
    stream=False
)

print(completion.to_json())