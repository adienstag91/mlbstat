import os
import ast
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict

class SimpleCodeAnalyzer:
    """
    Simple code analyzer that recursively scans directories and shows
    a clean tree structure with functions and classes.
    """
    
    def __init__(self, directory_path: str, exclude_folders=None):
        self.directory_path = Path(directory_path)
        if exclude_folders is None:
            exclude_folders = []
        self.exclude_folders = exclude_folders
        
    def get_functions_and_classes_from_file(self, file_path: str) -> Dict[str, Any]:
        """Extract functions, classes, and methods from a Python file."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()
                tree = ast.parse(content, filename=file_path)
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'functions': [],
                'classes': [],
                'methods_by_class': {}
            }
        
        functions = []
        classes = []
        methods_by_class = defaultdict(list)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if this function is inside a class
                parent_class = self._get_parent_class(node, tree)
                if parent_class:
                    methods_by_class[parent_class].append(node.name)
                else:
                    functions.append(node.name)
            elif isinstance(node, ast.ClassDef):
                classes.append(node.name)
        
        return {
            'status': 'success',
            'functions': functions,
            'classes': classes,
            'methods_by_class': dict(methods_by_class)
        }
    
    def _get_parent_class(self, func_node: ast.FunctionDef, tree: ast.AST) -> str:
        """Find the parent class of a function node."""
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if child == func_node:
                        return node.name
        return None
    
    def scan_directory_recursive(self) -> Dict[str, Any]:
        """Recursively scan directory for all Python files."""
        results = {}
        
        if not self.directory_path.exists():
            return {'error': f"Directory {self.directory_path} does not exist"}
        
        # Find all Python files recursively, excluding specified folders
        python_files = []
        for file_path in self.directory_path.rglob("*.py"):
            # Check if any part of the path matches excluded folders
            path_parts = file_path.relative_to(self.directory_path).parts
            if any(excluded in path_parts for excluded in self.exclude_folders):
                continue
            python_files.append(file_path)
        
        if not python_files:
            excluded_msg = f" (excluding: {', '.join(self.exclude_folders)})" if self.exclude_folders else ""
            return {'error': f"No Python files found in {self.directory_path}{excluded_msg}"}
        
        for file_path in python_files:
            # Create relative path for clean display
            relative_path = file_path.relative_to(self.directory_path)
            results[str(relative_path)] = self.get_functions_and_classes_from_file(str(file_path))
        
        return results
    
    def print_tree_structure(self):
        """Print a clean tree structure of all code."""
        results = self.scan_directory_recursive()
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return results
        
        # Show excluded folders in header
        excluded_msg = f" (excluding: {', '.join(self.exclude_folders)})" if self.exclude_folders else ""
        print(f"📁 {self.directory_path.name}/{excluded_msg}")
        
        # Organize by directory structure
        by_directory = defaultdict(list)
        for file_path, data in results.items():
            directory = str(Path(file_path).parent)
            if directory == '.':
                directory = 'ROOT'
            by_directory[directory].append((file_path, data))
        
        # Print tree structure
        for directory, files in sorted(by_directory.items()):
            if directory == 'ROOT':
                # Root level files
                for file_path, data in sorted(files):
                    filename = Path(file_path).name
                    self._print_file_contents(filename, data, "├── ")
            else:
                # Subdirectory
                print(f"├── 📁 {directory}/")
                for i, (file_path, data) in enumerate(sorted(files)):
                    filename = Path(file_path).name
                    is_last = i == len(files) - 1
                    prefix = "│   └── " if is_last else "│   ├── "
                    self._print_file_contents(filename, data, prefix)
        
        return results
    
    def _print_file_contents(self, filename: str, data: Dict, prefix: str):
        """Print the contents of a single file."""
        if data['status'] != 'success':
            print(f"{prefix}📄 {filename} ❌ {data['error']}")
            return
        
        print(f"{prefix}📄 {filename}")
        
        # Print standalone functions
        if data['functions']:
            for i, func in enumerate(data['functions']):
                is_last_func = i == len(data['functions']) - 1
                has_classes = bool(data['classes'])
                
                if is_last_func and not has_classes:
                    print(f"{prefix.replace('├──', '   ').replace('└──', '   ')}└── 🔹 {func}()")
                else:
                    print(f"{prefix.replace('├──', '   ').replace('└──', '   ')}├── 🔹 {func}()")
        
        # Print classes and their methods
        if data['classes']:
            for i, class_name in enumerate(data['classes']):
                is_last_class = i == len(data['classes']) - 1
                class_prefix = "└──" if is_last_class else "├──"
                
                methods = data['methods_by_class'].get(class_name, [])
                print(f"{prefix.replace('├──', '   ').replace('└──', '   ')}{class_prefix} 📦 {class_name}")
                
                # Print methods
                for j, method in enumerate(methods):
                    is_last_method = j == len(methods) - 1
                    method_prefix = "└──" if is_last_method else "├──"
                    base_indent = prefix.replace('├──', '   ').replace('└──', '   ')
                    class_indent = "    " if is_last_class else "│   "
                    print(f"{base_indent}{class_indent}{method_prefix} ⚙️ {method}()")

def main():
    """Main function to run the analysis."""
    # Use your actual directory path
    directory_path = "/Users/andrewdienstag/mlbstat/src"
    
    # Specify folders to exclude
    exclude_folders = [
        "backup_old_code", 
        "migration_files", 
        "__pycache__",
        "archive",
        ".git",
        "reprocessing_reports",
        "cache"
    ]
    
    analyzer = SimpleCodeAnalyzer(directory_path, exclude_folders)
    
    print("🔍 MLB Parser Code Structure:")
    print("=" * 50)
    
    results = analyzer.print_tree_structure()
    
    # Simple summary
    if results and 'error' not in results:
        total_files = len([r for r in results.values() if r['status'] == 'success'])
        total_functions = sum(len(r['functions']) for r in results.values() if r['status'] == 'success')
        total_classes = sum(len(r['classes']) for r in results.values() if r['status'] == 'success')
        total_methods = sum(
            sum(len(methods) for methods in r['methods_by_class'].values()) 
            for r in results.values() if r['status'] == 'success'
        )
        
        print("\n" + "=" * 50)
        print(f"📊 Summary: {total_files} files, {total_functions} functions, {total_classes} classes, {total_methods} methods")

if __name__ == "__main__":
    main()