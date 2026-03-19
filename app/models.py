from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Enum, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app import db

import enum


class TradeType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class SourceFormat(str, enum.Enum):
    # Values must match FileFormat values in app/ingestion/detector.py exactly
    # so that SourceFormat(file_format.value) works without a lookup table.
    TRADE_CSV     = "TRADE_CSV"       # comma-delimited daily trade fills
    TRADE_PIPE    = "TRADE_PIPE"      # pipe-delimited daily trade fills
    POSITION_YAML = "POSITION_YAML"   # YAML end-of-day position snapshot


class Trade(db.Model):
    __tablename__ = "trades"
    # DB-level unique constraint is the final guard against concurrent duplicate
    # inserts that pass the application-level existence check simultaneously.
    __table_args__ = (
        UniqueConstraint(
            "trade_date", "account_id", "ticker", "quantity", "price", "trade_type",
            name="uq_trade",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    # Signed quantity: positive = BUY, negative = SELL.
    # SUM(quantity) over all trades yields the correct net long/short position.
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    trade_type: Mapped[TradeType] = mapped_column(Enum(TradeType), nullable=False)
    settlement_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_format: Mapped[SourceFormat] = mapped_column(Enum(SourceFormat), nullable=False)

    def __repr__(self) -> str:
        return (f"<Trade {self.account_id} {self.ticker} "
                f"{self.trade_type} {self.quantity}@{self.price}>")


class Position(db.Model):
    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint(
            "report_date", "account_id", "ticker", "custodian_ref",
            name="uq_position",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    # Signed shares: negative = short position.
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    market_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    custodian_ref: Mapped[str] = mapped_column(String(100), nullable=True)
    source_format: Mapped[SourceFormat] = mapped_column(Enum(SourceFormat), nullable=False)

    def __repr__(self) -> str:
        return (f"<Position {self.account_id} {self.ticker} "
                f"{self.shares} shares @ {self.market_value}>")
