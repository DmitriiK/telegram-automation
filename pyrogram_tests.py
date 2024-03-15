from datetime import datetime
import asyncio
import os

from pyrogram import Client, types, filters
from dotenv import load_dotenv

load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
PHONE = '+79053862836'
app = Client("my_account", api_id=api_id, api_hash=api_hash,  phone_number=PHONE)
print(f'app started at {datetime.now()}')
 

async def main():
    async with app:
        # await app.send_message("me", "Hi!")
        chat_id = -1001688539638
        async for post in app.get_chat_history(chat_id=chat_id, limit=30):
            print(post)
            await asyncio.sleep(1)
         
app.run(main())