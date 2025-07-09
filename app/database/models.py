from sqlalchemy import BigInteger, String, ForeignKey, Boolean
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
    tg_id = mapped_column(BigInteger)
    nickname: Mapped[str] = mapped_column()
    norm_rate: Mapped[int] = mapped_column(default=os.getenv('NORM'))

class DailyMessage(Base):
    __tablename__ = 'daily_messages'

    tg_id = mapped_column(BigInteger, primary_key=True)
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