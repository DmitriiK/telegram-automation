
import datetime
import time
from sys import getsizeof
import os
from typing import List, Dict
import asyncio
import logging

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesReactionsRequest
from telethon.tl.types import ReactionEmoji

partitioning_column = 'msg_month_key'

logging.basicConfig(level=logging.INFO)
load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
client = TelegramClient('anon', api_id, api_hash, system_version='4.16.30-vxDKLMN')
# need to specify any value for system_version, othervise you'll loose all you other sessions on all devices
# https://github.com/LonamiWebs/Telethon/issues/4051


def __extract_message_data(msg) -> Dict:
    col_maps = {
        ("msg_id", lambda mes: mes.id),
        ("user_id", lambda mes: mes.from_id.user_id),
        ("user_name", lambda mes: mes.sender.first_name
         if msg.sender else None),
        ("msg_date", lambda msg: msg.date),
        (partitioning_column, lambda msg: msg.date.year*100 + msg.date.month),  # key for partitioning
        ("reply_to_msg_id", lambda msg: msg.reply_to.reply_to_msg_id
            if msg.reply_to and msg.reply_to.reply_to_msg_id else None),
        ("msg_text", lambda msg: msg.message),
        ("react_cnt", lambda msg: len(msg.reactions.results)
         if msg.reactions and msg.reactions.results else 0),
    }
    mess_data = {}
    for atr, fnc in col_maps:
        mess_data[atr] = fnc(msg)
    return mess_data


async def extract_messages(chat_id: int, limit: int = 1000):
    ret = []
    msg_dt, cnt_by_dt, cnt_all = None, 0, 0
    async for msg in client.iter_messages(chat_id,  wait_time=1, limit=limit):
        cnt_all += 1
        cnt_by_dt += 1
        if msg.date.date() != msg_dt:
            logging.info(f"got {cnt_by_dt=}/{cnt_all=} messages for {msg_dt}. total size in memory is {getsizeof(ret)} bytes")
            msg_dt = msg.date.date()
            cnt_by_dt = 0
        d_i = __extract_message_data(msg)
        ret.append(d_i)
    logging.info(f"got {cnt_by_dt} messages for {msg_dt}. total size in memory is {getsizeof(ret)} bytes")
    return ret


async def extract_message_reactions(chat_id: int, mes_ids: List[int], chunk_size=100) -> pd.DataFrame:
    react_lst = []
    logging.info(f'need to process {len(mes_ids)} messages')
    async with client:
        n_of_chunks = int(len(mes_ids)/chunk_size)
        for n_cnunk in range(n_of_chunks+1):
            chunk_of_ids = mes_ids[n_cnunk*chunk_size: (n_cnunk+1)*chunk_size]
            logging.info(f'retrieving reactions for {len(chunk_of_ids)} messages')
            rr = await client(GetMessagesReactionsRequest(chat_id, id=chunk_of_ids))
            logging.info(f'got reactions for {len(rr.updates)} messages at {datetime.datetime.now().time()}')
            for msg in rr.updates:
                for react in msg.reactions.results:
                    emo = react.reaction.emoticon if isinstance(react.reaction,ReactionEmoji) else 'custom'
                    react_lst.append(
                        {
                            'msg_id': int(msg.msg_id),
                            'cnt': react.count,
                            'emo': emo            
                        }
                    )
            time.sleep(2)
        # print(react_lst)
        df = pd.DataFrame.from_dict(react_lst)
        return df


def test_reactions():
    ids = [156714, 156713, 156712, 156711, 156710, 156709, 156708, 156707, 156706, 156705, 156704, 156703, 156702, 156701, 156700, 156699, 156698, 156697, 156696, 156695, 156694, 156693, 156692, 156691, 156690, 156689, 156688, 156687, 156686, 156685, 156684, 156683, 156682, 156681, 156680, 156679, 156678, 156677, 156676, 156675, 156674, 156673, 156672, 156671, 156670, 156669, 156668, 156667, 156666, 156665, 156664, 156663, 156662, 156661, 156660, 156659, 156658, 156657, 156656]
    tlg_group_id = -1001688539638
    xx = asyncio.run(extract_message_reactions(tlg_group_id, ids, chunk_size=10))
    print(f'got shape {xx.shape}')
    print(xx)


async def test_extraction():
    tlg_group_id = -1001688539638
    xx = await extract_messages(chat_id=tlg_group_id, limit=1000000)
    df = pd.DataFrame.from_dict(xx)
    df.to_parquet(f'chat{tlg_group_id}.parquet.gzip', compression='gzip', partition_cols=[partitioning_column])
    # print(xx)
    

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(test_extraction())


# %%
