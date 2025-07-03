import re

text = "NewYorkYankees"
fixed = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
print(fixed)  # Output: "New York Yankees"