
# %%
import pandas as pd
import datetime
import ijson


def telega_dump_to_pandas(dump_path: str) -> pd.DataFrame:
    with open(dump_path, "rb") as f:
        msgs = (msg for msg in ijson.items(f, 'messages.item')
                if msg['type'] != 'service')
        data = []
        for msg in msgs:
            mess_data = __extract_message_data(msg)
            data.append(mess_data)
            # new_row = pd.DataFrame([mess_data])
            # df = pd.concat([df, new_row])
        df = pd.DataFrame.from_dict(data)
        return df


def __extract_message_data(msg):
    mess_data = {'msg_id': int(msg.get('id', '')),
                 'msg_date': datetime.datetime.strptime(msg['date'], "%Y-%m-%dT%H:%M:%S"),
                 'user_name': str(msg.get('from', '')) or '',
                 'user_id': str(msg.get('from_id', '')) or '',
                 'reply_to_msg_id': str(msg.get('reply_to_message_id', '')) or ''
                 }
    tes = msg['text_entities']
    msg_parts = [mp['text'] for mp in tes]
    text = ''.join(msg_parts)
    mess_data["msg_len"] = len(text)
    mess_data["msg_text"] = text
    return mess_data



# %%
if __name__ == "__main__":
    dump_path = r"D:\test_data\ChatExport_2024-03-14\result.json"
    df = telega_dump_to_pandas(dump_path=dump_path)
    

# %%
df

# %%
