from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import inspect
import improved_mlb_parser

def test_available_functions():
    """Check what functions are available in the parser module"""
    
    print("🔍 AVAILABLE FUNCTIONS IN improved_mlb_parser:")
    
    # Get all functions from the module
    functions = [name for name, obj in inspect.getmembers(improved_mlb_parser) 
                if inspect.isfunction(obj)]
    
    for func_name in functions:
        func = getattr(improved_mlb_parser, func_name)
        sig = inspect.signature(func)
        print(f"  {func_name}{sig}")
    
    print("\n🧪 TESTING GAME-LEVEL PARSING:")
    
    # Test with actual data
    test_url = "https://www.baseball-reference.com/boxes/NYA/NYA202506080.shtml"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(test_url)
        html = page.content()
        browser.close()
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try the main function from your working script
    if hasattr(improved_mlb_parser, 'main'):
        print("🧪 Found main() function - this might be what we need!")
        try:
            result = improved_mlb_parser.main()
            print(f"✅ main() returned: {type(result)}")
        except Exception as e:
            print(f"❌ main() failed: {e}")
    
    # Look for functions that might parse the whole game
    potential_functions = [name for name in functions if 
                          'game' in name.lower() or 
                          'analyze' in name.lower() or 
                          'parse' in name.lower()]
    
    print(f"\n🎯 POTENTIAL GAME-LEVEL FUNCTIONS: {potential_functions}")

if __name__ == "__main__":
    test_available_functions()