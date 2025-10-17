# tests/test_app.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

BASE = "http://localhost:5000"

def _driver():
    opts = Options()
    opts.add_argument("--headless")  # comment out if you want to see the browser
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def test_search_page():
    d = _driver()
    d.get(f"{BASE}/search")
    assert "Search NYC 311" in d.page_source
    d.quit()

def test_summary_page():
    d = _driver()
    d.get(f"{BASE}/summary")
    assert "Complaints per Borough" in d.page_source
    d.quit()
