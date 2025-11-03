from sqlalchemy import BigInteger, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine

engine = create_async_engine(url='sqlite+aiosqlite:///db.sqlite3', echo=False)
async_session = async_sessionmaker(engine)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = 'agents'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    nickname: Mapped[str] = mapped_column(String)
    norm_rate: Mapped[int] = mapped_column(default=0)


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


class Admin(Base):
    __tablename__ = 'admins'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(40))
    is_creator: Mapped[int] = mapped_column()


# Новые таблицы для групп агентов
class AgentGroup(Base):
    __tablename__ = 'agent_groups'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(100), unique=True)  # уникальное название группы/агента


class AgentGroupMember(Base):
    __tablename__ = 'agent_group_members'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column()  # ссылка на AgentGroup.id
    agent_nickname: Mapped[str] = mapped_column(String(100), unique=True)  # ник состоит только в одной группе


# Глобальные настройки выплат и норм
class Settings(Base):
    __tablename__ = 'settings'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rate_per_dialog: Mapped[int] = mapped_column(default=20)  # базовая ставка за диалог
    norms_enabled: Mapped[int] = mapped_column(default=0)     # 0=выкл, 1=вкл глобально
    global_norm_rate: Mapped[int] = mapped_column(default=20) # ставка при глобальной норме
    top_bonus: Mapped[int] = mapped_column(default=300)       # премия за наибольшее количество диалогов (2 недели)


# Корректировки диалогов (не удаляют клиентов)
class AgentAdjustment(Base):
    __tablename__ = 'agent_adjustments'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_id: Mapped[int] = mapped_column(BigInteger)
    date: Mapped[str] = mapped_column(String(20))
    delta: Mapped[int] = mapped_column(default=0)  # отрицательное число для вычитания
    reason: Mapped[str] = mapped_column(String(200), default="")


async def async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
