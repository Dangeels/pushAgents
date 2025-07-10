from sqlalchemy import BigInteger, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
import os

engine = create_async_engine(url='sqlite+aiosqlite:///db.sqlite3', echo=False)
async_session = async_sessionmaker(engine)

class Base(AsyncAttrs, DeclarativeBase):
    pass

class Agent(Base):
    __tablename__ = 'agents'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    nickname: Mapped[str] = mapped_column(String)
    norm_rate: Mapped[int] = mapped_column(default=int(os.getenv('NORM', '15')))

class DailyMessage(Base):
    __tablename__ = 'daily_messages'
    tg_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    date: Mapped[str] = mapped_column(String(20), primary_key=True)
    dialogs_count: Mapped[int] = mapped_column(default=0)
    salary: Mapped[int] = mapped_column(default=0)

class Client(Base):
    __tablename__ = 'clients'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(40))

async def async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)