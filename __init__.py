import asyncio
import io
import sqlite3
import urllib.parse

import aiohttp
import discord
import imagehash
from PIL import Image
from discord.ext import commands

import breadcord


class RepostScolder(breadcord.module.ModuleCog):
    def __init__(self, module_id: str, /):
        super().__init__(module_id)
        self.session: aiohttp.ClientSession = None  # type: ignore

        self.connection = sqlite3.connect(self.module.storage_path / "images.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS image_hashes (hash TEXT PRIMARY KEY NOT NULL UNIQUE)")
        self.connection.commit()

    async def cog_load(self) -> None:
        self.session = aiohttp.ClientSession()

    async def cog_unload(self):
        if self.session is not None:
            await self.session.close()

    async def fetch_image(self, image_url: str, /) -> io.BytesIO | None:
        async with self.session.get(image_url) as response:
            return io.BytesIO(await response.read()) if response.status == 200 else None

    def get_image_hash(self, image: io.BytesIO, /) -> str:
        image: Image.Image = Image.open(image)
        return str(imagehash.average_hash(image, self.settings.hash_size.value))

    async def is_dupe(self, image_url: str) -> bool:
        fetched_image = await self.fetch_image(image_url)
        if fetched_image is None:
            return False

        image_hash = await asyncio.gather(asyncio.to_thread(self.get_image_hash, fetched_image))
        image_hash = str(image_hash[0])
        exists = bool(
            self.cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM image_hashes WHERE hash = ?)",
                (image_hash,),
            ).fetchone()[0]
        )

        if not exists:
            self.cursor.execute("INSERT INTO image_hashes VALUES (?)", (image_hash,))
            self.connection.commit()

        return exists

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if (
            message.channel.id not in self.settings.allowed_channels.value
            or self.settings.ignore_bots.value
        ):
            return

        image_urls = [attachment.url for attachment in message.attachments]
        image_urls.extend(embeds.url for embeds in message.embeds)
        image_urls = list(filter(lambda x: x, dict.fromkeys(image_urls)))

        for url in image_urls:
            url_path = urllib.parse.urlparse(url).path
            file_extension = url_path.split(".")[-1]
            if file_extension not in self.settings.accepted_file_formats.value:
                continue

            is_dupe = await self.is_dupe(url)
            if is_dupe:
                await message.reply(self.settings.scold_message.value)


async def setup(bot: breadcord.Bot):
    await bot.add_cog(RepostScolder("repost_scolder"))
