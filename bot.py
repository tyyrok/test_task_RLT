import asyncio
import logging
import sys
import json
from os import getenv
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold

from aggregate_query import aggregate

if not load_dotenv('.env.txt'):
    raise FileNotFoundError

INCORRECT_INPUT = """Некорректный запрос. Пример запроса:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", 
"group_type": "month"}"""

TOKEN = getenv("BOT_TOKEN")

dp = Dispatcher()

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    This handler receives messages with `/start` command
    """
    await message.answer(f"Hello, {hbold(message.from_user.full_name)}!")

@dp.message()
async def echo_handler(message: types.Message) -> None:
    """Simple handler that takes input data and return incorrect 
    input message or aggregated data
    """
    print(message.text)
    raw_data = message.text
    try:
        data = json.loads(raw_data)
        res = await aggregate(data)
        if not res:
            await message.answer(INCORRECT_INPUT)
        else:
            string = json.dumps(res)
            if len(string) >= 4000:
                s = 0
                while s < len(string):
                    if s + 3990 > len(string):
                        await message.answer(string[s:])
                    else:
                        await message.answer(string[s:s+3999])
                    s += 3999
                    await asyncio.sleep(0.1)
            else:
                await message.answer(string)
                
    except json.JSONDecodeError:
        await message.answer(INCORRECT_INPUT)


async def main() -> None:
    """ Initialize Bot instance with a default parse mode which 
    will be passed to all API calls
    """
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    await dp.start_polling(bot)
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())