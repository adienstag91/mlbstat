import os
import ast
from typing import Dict, List, Tuple
import traceback

class CodeAnalyzer:
    def __init__(self, directory_path: str):
        self.directory_path = directory_path
    
    def get_functions_and_classes_from_file(self, file_path: str) -> Dict:
        """Extracts functions, classes, and methods from a Python file with error handling."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()
                tree = ast.parse(content, filename=file_path)
            
            functions = []
            classes = []
            methods_by_class = {}
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    # Check if this function is inside a class
                    parent_class = self._get_parent_class(node, tree)
                    if parent_class:
                        if parent_class not in methods_by_class:
                            methods_by_class[parent_class] = []
                        methods_by_class[parent_class].append(node.name)
                    else:
                        functions.append(node.name)
                elif isinstance(node, ast.ClassDef):
                    classes.append(node.name)
            
            return {
                'status': 'success',
                'functions': functions,
                'classes': classes,
                'methods_by_class': methods_by_class,
                'error': None
            }
            
        except SyntaxError as e:
            return {
                'status': 'syntax_error',
                'functions': [],
                'classes': [],
                'methods_by_class': {},
                'error': f"Syntax Error at line {e.lineno}: {e.msg}"
            }
        except Exception as e:
            return {
                'status': 'error',
                'functions': [],
                'classes': [],
                'methods_by_class': {},
                'error': str(e)
            }
    
    def _get_parent_class(self, func_node, tree):
        """Find if a function is inside a class."""
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if child == func_node:
                        return node.name
        return None
    
    def analyze_directory(self) -> Dict:
        """Analyze all Python files in the directory."""
        results = {}
        
        if not os.path.exists(self.directory_path):
            return {'error': f"Directory {self.directory_path} does not exist"}
        
        python_files = [f for f in os.listdir(self.directory_path) if f.endswith('.py')]
        
        if not python_files:
            return {'error': f"No Python files found in {self.directory_path}"}
        
        for filename in python_files:
            file_path = os.path.join(self.directory_path, filename)
            results[filename] = self.get_functions_and_classes_from_file(file_path)
        
        return results
    
    def print_summary(self):
        """Print a nice summary of all code structure."""
        results = self.analyze_directory()
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
        
        print(f"📁 Code Analysis for: {self.directory_path}")
        print("=" * 60)
        
        total_files = len(results)
        successful_files = sum(1 for r in results.values() if r['status'] == 'success')
        error_files = total_files - successful_files
        
        print(f"📊 Summary: {successful_files}/{total_files} files parsed successfully")
        if error_files > 0:
            print(f"⚠️  {error_files} files had parsing errors")
        print()
        
        for filename, data in results.items():
            print(f"📂 {filename}")
            
            if data['status'] != 'success':
                print(f"   ❌ {data['error']}")
                print()
                continue
            
            # Print standalone functions
            if data['functions']:
                print("   🔹 Functions:")
                for func in data['functions']:
                    print(f"      • {func}()")
            
            # Print classes and their methods
            if data['classes']:
                print("   🏗️  Classes:")
                for class_name in data['classes']:
                    print(f"      📦 {class_name}")
                    if class_name in data['methods_by_class']:
                        for method in data['methods_by_class'][class_name]:
                            print(f"         ⚙️  {method}()")
            
            if not data['functions'] and not data['classes']:
                print("   📝 No functions or classes found")
            
            print()
    
    def get_refactoring_suggestions(self):
        """Suggest refactoring opportunities."""
        results = self.analyze_directory()
        
        if 'error' in results:
            return
        
        print("🔧 Refactoring Suggestions:")
        print("=" * 40)
        
        for filename, data in results.items():
            if data['status'] != 'success':
                continue
            
            suggestions = []
            
            # Check for files with many standalone functions
            if len(data['functions']) > 5:
                suggestions.append(f"Consider grouping {len(data['functions'])} functions into classes")
            
            # Check for classes with many methods
            for class_name, methods in data['methods_by_class'].items():
                if len(methods) > 10:
                    suggestions.append(f"Class '{class_name}' has {len(methods)} methods - consider splitting")
            
            # Check for files with mixed paradigms
            if data['functions'] and data['classes']:
                suggestions.append("Mixed functions and classes - consider consistent approach")
            
            if suggestions:
                print(f"📂 {filename}:")
                for suggestion in suggestions:
                    print(f"   💡 {suggestion}")
                print()

def fix_indentation_errors(directory_path: str):
    """Helper function to identify and fix common indentation issues."""
    print("🔍 Checking for indentation issues...")
    
    for filename in os.listdir(directory_path):
        if not filename.endswith('.py'):
            continue
            
        file_path = os.path.join(directory_path, filename)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Try to parse - this will catch syntax errors
            ast.parse(content, filename=filename)
            
        except SyntaxError as e:
            print(f"❌ {filename}: Syntax error at line {e.lineno}")
            print(f"   Error: {e.msg}")
            
            # Show the problematic line
            lines = content.split('\n')
            if e.lineno <= len(lines):
                problematic_line = lines[e.lineno - 1]
                print(f"   Line {e.lineno}: '{problematic_line}'")
                
                # Check for common indentation issues
                if 'unindent does not match' in e.msg:
                    print("   💡 This looks like an indentation mismatch.")
                    print("   💡 Check that spaces/tabs are consistent.")
                    
                    # Show surrounding lines for context
                    start = max(0, e.lineno - 3)
                    end = min(len(lines), e.lineno + 2)
                    print("   📝 Context:")
                    for i in range(start, end):
                        marker = ">>> " if i == e.lineno - 1 else "    "
                        print(f"   {marker}{i+1}: {lines[i]}")
            print()

# Usage
if __name__ == "__main__":
    # Replace with your actual directory
    directory_path = "/Users/andrewdienstag/mlbstat/src"
    
    # First, check for syntax errors
    fix_indentation_errors(directory_path)
    
    # Then analyze the code structure
    analyzer = CodeAnalyzer(directory_path)
    analyzer.print_summary()
    
    # Get refactoring suggestions
    analyzer.get_refactoring_suggestions()
