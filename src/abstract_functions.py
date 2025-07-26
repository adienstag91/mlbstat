import os
import ast

def get_functions_from_file(file_path):
    """Extracts function names from a given Python file."""
    with open(file_path, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=file_path)
    
    functions = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    return functions

def get_all_functions(directory):
    """Loops through all .py files in a directory and returns their function names."""
    functions_by_file = {}

    for filename in os.listdir(directory):
        if filename.endswith(".py"):
            file_path = os.path.join(directory, filename)
            functions_by_file[filename] = get_functions_from_file(file_path)

    return functions_by_file

# Replace with your actual directory containing Python files
directory_path = "/Users/andrewdienstag/mlbstat/src"  
functions_by_file = get_all_functions(directory_path)

# Print results
for file, functions in functions_by_file.items():
    print(f"\n📂 {file}:")
    for func in functions:
        print(f"   🔹 {func}")
