from fastapi import FastAPI
from Utils.WebScraping import WebScraping

app = FastAPI()

@app.get("/links")
async def get_website_links():
    website_content = WebScraping()
    return website_content