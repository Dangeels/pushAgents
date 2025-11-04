from sqlalchemy import BigInteger, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine

engine = create_async_engine(url='sqlite+aiosqlite:///db.sqlite3', echo=False)
# Отключаем истечение атрибутов после commit, чтобы не провоцировать ленивые загрузки (greenlet_spawn)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = 'agents'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tg_id: Mapped[int | None] = mapped_column(BigInteger, unique=True, nullable=True)
    nickname: Mapped[str] = mapped_column(String)
    norm_rate: Mapped[int] = mapped_column()
    # Признак включённых норм для агента (1=включены, 0=выключены)
    norms_enabled: Mapped[int] = mapped_column(default=1)


class AgentAccount(Base):
    __tablename__ = 'agent_accounts'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agent_id: Mapped[int] = mapped_column(ForeignKey('agents.id'))
    tg_id: Mapped[int | None] = mapped_column(BigInteger, unique=True, nullable=True)
    tg_username: Mapped[str | None] = mapped_column(String(40), nullable=True)


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


class Norm(Base):
    __tablename__ = 'norm_rate'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    norm: Mapped[int] = mapped_column()
    salary: Mapped[int] = mapped_column()
    bonuses: Mapped[int] = mapped_column()
    week_norm_bonuses: Mapped[int] = mapped_column()
    best_week_agent: Mapped[int] = mapped_column()
    # Глобальный признак включённых норм (1=включены, 0=выключены)
    norms_enabled_global: Mapped[int] = mapped_column(default=1)
    # Стоимость одного диалога при отключённых нормах
    dialog_price: Mapped[int] = mapped_column(default=20)


async def _migrate_agents_tg_nullable():
    async with engine.begin() as conn:
        # Проверим признак NOT NULL у столбца tg_id
        info = await conn.exec_driver_sql("PRAGMA table_info(agents)")
        rows = info.fetchall()
        if not rows:
            return
        # Найдём tg_id
        tg_row = next((r for r in rows if r[1] == 'tg_id'), None)
        if tg_row is None:
            return
        notnull = tg_row[3]  # 1 = NOT NULL, 0 = допускает NULL
        if notnull == 0:
            return
        # Миграция: делаем tg_id допускающим NULL
        await conn.exec_driver_sql("PRAGMA foreign_keys=off;")
        await conn.exec_driver_sql(
            """
            CREATE TABLE agents_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id BIGINT UNIQUE,
                nickname VARCHAR,
                norm_rate INTEGER,
                norms_enabled INTEGER DEFAULT 1
            );
            """
        )
        await conn.exec_driver_sql(
            "INSERT INTO agents_new (id, tg_id, nickname, norm_rate, norms_enabled) SELECT id, tg_id, nickname, norm_rate, 1 FROM agents;"
        )
        await conn.exec_driver_sql("DROP TABLE agents;")
        await conn.exec_driver_sql("ALTER TABLE agents_new RENAME TO agents;")
        await conn.exec_driver_sql("PRAGMA foreign_keys=on;")


async def _migrate_add_norms_flags():
    """Добавляет недостающие поля norms_enabled в agents и norms_enabled_global в norm_rate."""
    async with engine.begin() as conn:
        # agents.norms_enabled
        info = await conn.exec_driver_sql("PRAGMA table_info(agents)")
        cols = [r[1] for r in info.fetchall()] if info else []
        if 'norms_enabled' not in cols:
            await conn.exec_driver_sql("ALTER TABLE agents ADD COLUMN norms_enabled INTEGER DEFAULT 1")
        # norm_rate.norms_enabled_global
        info2 = await conn.exec_driver_sql("PRAGMA table_info(norm_rate)")
        cols2 = [r[1] for r in info2.fetchall()] if info2 else []
        if 'norms_enabled_global' not in cols2:
            await conn.exec_driver_sql("ALTER TABLE norm_rate ADD COLUMN norms_enabled_global INTEGER DEFAULT 1")


async def _migrate_add_dialog_price():
    """Добавляет столбец dialog_price в norm_rate при отсутствии."""
    async with engine.begin() as conn:
        info = await conn.exec_driver_sql("PRAGMA table_info(norm_rate)")
        cols = [r[1] for r in info.fetchall()] if info else []
        if 'dialog_price' not in cols:
            await conn.exec_driver_sql("ALTER TABLE norm_rate ADD COLUMN dialog_price INTEGER DEFAULT 20")


async def async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Миграции
    await _migrate_agents_tg_nullable()
    await _migrate_add_norms_flags()
    await _migrate_add_dialog_price()

    # Backfill: гарантируем запись в agent_accounts для каждого агента с tg_id
    from sqlalchemy import select
    async with async_session() as session:
        try:
            agents = await session.scalars(select(Agent))
            for ag in agents:
                if not ag.tg_id:
                    continue
                exists = await session.scalar(select(AgentAccount).where(AgentAccount.tg_id == ag.tg_id))
                if not exists:
                    session.add(AgentAccount(agent_id=ag.id, tg_id=ag.tg_id, tg_username=ag.nickname))
            await session.commit()
        except Exception:
            await session.rollback()
