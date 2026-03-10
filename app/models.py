from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Enum, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app import db

import enum

# TradeType Class
class TradeType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"

# Source Format Class
class SourceFormat(str, enum.Enum):
    FORMAT_1 = "FORMAT_1"  # CSV comma-delimited
    FORMAT_2 = "FORMAT_2"  # pipe-delimited

# Trade Class
class Trade(db.Model):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    trade_type: Mapped[TradeType] = mapped_column(Enum(TradeType), nullable=False)
    settlement_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_format: Mapped[SourceFormat] = mapped_column(Enum(SourceFormat), nullable=False)

    def __repr__(self) -> str:
        return f"<Trade {self.account_id} {self.ticker} {self.trade_type} {self.quantity}@{self.price}>"

# Position class
class Position(db.Model):
    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint("report_date", "account_id", "ticker", "custodian_ref", name="uq_position"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    market_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    custodian_ref: Mapped[str] = mapped_column(String(100), nullable=True)
    source_format: Mapped[str] = mapped_column(String(20), nullable=False, default="FORMAT_3")

    def __repr__(self) -> str:
        return f"<Position {self.account_id} {self.ticker} {self.shares} shares @ {self.market_value}>"
