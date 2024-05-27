import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

import config
from mongo_query import process_query

bot = Bot(token=config.TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: Message):
    user_name = message.from_user.first_name
    user_id = message.from_user.id
    await message.answer(
        text=f"Hi [{user_name}](tg://user?id={str(user_id)})!", parse_mode="Markdown"
    )


@dp.message()
async def mongo(message: Message):
    result = process_query(message.text)
    await message.answer(str(result))


async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
