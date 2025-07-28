from groq import Groq
import json


getNameWeight_prompt = """ 
Retrieve a player's name and weight from the following text and return it in CSV format with these headers everytime: 'name', 'wt'. Ensure the following formatting rules:

***name***: Input player's name; no jersey number. If missing, leave the field blank.

***wt***: Provide only the number in pounds (e.g., 180). If missing, leave the field blank ("").

***Ensure***:
    - Avoid including non-player info, comment, or extra text. Only return the information in CSV format.
    - There should only be two fields.

Here is the text:


"""

shorter_prompt = """ 
Retrieve player information from the following text and return it in CSV format with these headers every time: Player, Position, Height, Weight, Year, Home_City, Home_State_Country. Ensure the following formatting rules:

1. **Player**: Input player's name; no jersey number.

2. **Home_State_Country**: Include abbreivated state (2 letters max with no period) for USA players and full country for international players.

3. **Height**: Format as feet-inches (e.g., 6-2). If height is missing, leave the field blank ("").

4. **Position**: Use abbreviations: G (Guard), F (Forward), C (Center). If position is missing, leave the field blank ("").

5. **Weight**: Provide only the number in pounds (e.g., 180). If missing, leave the field blank ("").

6. **Year**: Use abbreviations: Fr (Freshman), So (Sophomore), Jr (Junior), Sr (Senior), Gr (Graduate), or R- for redshirt. If missing, leave the field blank ("").

7. **Ensure**: 
    - If any field is missing or not provided, return an empty value ("").
    - Avoid including non-player info, comments, or extra text. Only return the information in CSV format.
    - Use consistent and clear formatting for all fields.
    - There should only be seven fields

Here is the text:


"""

longer_prompt = """
Retrieve player information for the following text and return it in CSV format:

- Player
- Position
- Height
- Weight
- Year
- Hometown

The CSV should have the following headers: "Player", "Position", "Height", :, "Year", "Hometown". Do not enclose with double quotations unless the field contains a comma. For example,
"Bryan Trimble, Jr." should have quotes around it, but "6-2" should not.
***ENSURE TO INCLUDE HEADERS IN THE CSV***

- For hometown, if there is an international player, leave it blank. If the player is from the US, format it doing city, abbreivated state (i.e Tucson, AZ)
- For homestate, just include state abbreviation (e.g., IL for Illinois).
- For height, format it as feet-inches (e.g., 6-2 instead of 6'2").
- For position, use the following abbreviations: G for Guard, F for Forward, and C for Center.
- For weight, only the number in pounds (e.g., 180, not 180 lbs).
- For year, use the following abbreviations: Fr for Freshman, So for Sophomore, Jr for Junior, Sr for Senior, Gr for Graduate Student, and leave "R-" for redshirt players.
- If any information is missing, leave the corresponding field blank but fill the position (e.g., "", not "N/A"). This will be fed into a csv() function in Python, so make sure the CSV is formatted correctly.
- Do not include non-player information or comments about it.

***DO NOT WRITE EXTRA COMMENTS OR NOTES!!!!! ONLY RETURN THE PLAYER DATA***

***If someone is not available, do not write a comment about them***

Here is the text:


"""

with open('OldPlayerInformationExtraction/apikeys.json', 'r')as file:
    data = json.load(file)['Groq']

    sethrbasketball_apikey = data['sethr']
    ohack_apikey = data['ohack']
    skrojas_apikey = data['skr']

def get_player_info_str(text, prompt = getNameWeight_prompt):

    client = Groq(
    api_key= sethrbasketball_apikey,
    )

    content = prompt + text

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": content,
            }
        ],
        model="llama-3.3-70b-versatile",
    )

    llm_return = chat_completion.choices[0].message.content

    return llm_return