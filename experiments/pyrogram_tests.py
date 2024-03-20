from datetime import datetime
import asyncio
import os
from typing import List
import logging

from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.raw.functions.messages import GetMessagesReactions

logging.basicConfig(level=logging.INFO)
load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('PHONE')

app = Client("my_account", api_id=api_id, api_hash=api_hash,  phone_number=PHONE)
print(f'app started at {datetime.now()}')


async def get_reactions(chat_id: int, mess_ids: List[int]):
    async with app:
        gr = await app.resolve_peer(chat_id)
        logging.info(f'retrieving reactions for {len(mess_ids)} messages')
        rr = await app.invoke(GetMessagesReactions(peer=gr, id=mess_ids))
        react_lst = []
        logging.info(f'got reactions for {len(rr.updates)} messages at {datetime.now().time()}')
        for msg in rr.updates:
            for react in msg.reactions.results:
                # emo = react.reaction.emoticon if isinstance(react.reaction,ReactionEmoji) else 'custom'
                react_lst.append(int(msg.msg_id))
        print(react_lst)
        return react_lst


async def test_reactions():
    await get_reactions(chat_id=-1001688539638, mess_ids=[149230])
    

async def main():
    async with app:
        # await app.send_message("me", "Hi!")
        chat_id = -1001688539638
        async for post in app.get_chat_history(chat_id=chat_id, limit=100):
            print('len, ', len(str(post)))
            if 'react' in str(post):
                print('react')
                print(post)
            # await asyncio.sleep(1)
            
# %%
if __name__ == "__main__":    
    app.run(main())