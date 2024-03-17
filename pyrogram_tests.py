from datetime import datetime
import asyncio
import os
from typing import List

from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.raw.functions.messages import GetMessagesReactions

load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('PHONE')

app = Client("my_account", api_id=api_id, api_hash=api_hash,  phone_number=PHONE)
print(f'app started at {datetime.now()}')


async def get_reactions(chat_id: int, mess_ids: List[int]):
    async with app:
        gr = await app.resolve_peer(chat_id)
        rr = await app.invoke(GetMessagesReactions(peer=gr, id=mess_ids))
        print(rr)


async def main():
    async with app:
        # await app.send_message("me", "Hi!")
        chat_id = -1001688539638
        async for post in app.get_chat_history(chat_id=chat_id, limit=30):
            print(post)
            await asyncio.sleep(1)
         
app.run(get_reactions(chat_id=-1001688539638, mess_ids=[149230]))