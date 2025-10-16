# db_model.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, UniqueConstraint, inspect
from sqlalchemy.orm import declarative_base, sessionmaker
import yaml
import os
import datetime

# --- 1. Lê o arquivo config.yaml ---
def carregar_config():
    caminho_config = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(caminho_config, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

config = carregar_config()
DB_URL = config["neon"]["DB_URL"]

# --- 2. Cria engine e sessão ---
engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- 3. Modelo da tabela de cotações atuais ---
class CotacaoAcao(Base):
    __tablename__ = "cotacoes_acoes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    Acao = Column(String, nullable=False)
    Hora = Column(String)
    Ultima = Column(Float)
    VarDia = Column(Float)
    VarSemana = Column(Float)
    VarMes = Column(Float)
    VarAno = Column(Float)
    MinDia = Column(Float)
    MaxDia = Column(Float)
    Volume = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return f"<CotacaoAcao(Acao='{self.Acao}', Ultima={self.Ultima})>"

# --- 4. Modelo da tabela de histórico ---
class HistoricoTicker(Base):
    __tablename__ = "historico_tickers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(DateTime, nullable=False)
    Open = Column(Float)
    High = Column(Float)
    Low = Column(Float)
    Close = Column(Float)
    Volume = Column(Float)
    Dividends = Column(Float)
    Stock_Splits = Column(Float)
    Ticker = Column(String, nullable=False)
    Capital_Gains = Column(String)

    __table_args__ = (
        UniqueConstraint('Date', 'Ticker', name='uix_date_ticker'),
    )

    def __repr__(self):
        return f"<HistoricoTicker(Ticker='{self.Ticker}', Date='{self.Date.date()}', Close={self.Close})>"

# --- 5. Funções utilitárias ---
def create_db_and_tables():
    Base.metadata.create_all(bind=engine)
    print("Banco e tabelas criados/verificados com sucesso.")

def get_db():
    return SessionLocal()

def table_exists(table_name):
    inspector = inspect(engine)
    return inspector.has_table(table_name)