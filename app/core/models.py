from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from flask_login import UserMixin
from sqlalchemy import CheckConstraint, Enum as SAEnum, ForeignKey, Index, UniqueConstraint, event, inspect, text
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from werkzeug.security import generate_password_hash

from app.core.extensions import db


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SepulturaEstado(str, Enum):
    # Spec 9.4.2 - Canvi de l'estat de les sepultures
    LLIURE = "LLIURE"
    DISPONIBLE = "DISPONIBLE"
    OCUPADA = "OCUPADA"
    INACTIVA = "INACTIVA"
    PROPIA = "PROPIA"


class DerechoTipo(str, Enum):
    # Spec 9.1.7 - Contractació del dret funerari
    CONCESION = "CONCESION"
    USO_INMEDIATO = "USO_INMEDIATO"


class TicketEstado(str, Enum):
    # Spec 9.1.3 + 5.3.4 - Cobrament de taxes
    PENDIENTE = "PENDIENTE"
    FACTURADO = "FACTURADO"
    COBRADO = "COBRADO"


class TicketDescuentoTipo(str, Enum):
    NONE = "NONE"
    PENSIONISTA = "PENSIONISTA"


class MovimientoTipo(str, Enum):
    INHUMACION = "INHUMACION"
    EXHUMACION = "EXHUMACION"
    TASAS = "TASAS"
    LAPIDA = "LAPIDA"
    CAMBIO_ESTADO = "CAMBIO_ESTADO"
    CONTRATO = "CONTRATO"
    INSCRIPCION_LATERAL = "INSCRIPCION_LATERAL"
    INICIO_TRANSMISION = "INICIO_TRANSMISION"
    DOCUMENTO_SUBIDO = "DOCUMENTO_SUBIDO"
    APROBACION = "APROBACION"
    RECHAZO = "RECHAZO"
    CAMBIO_TITULARIDAD = "CAMBIO_TITULARIDAD"


class InvoiceEstado(str, Enum):
    BORRADOR = "BORRADOR"
    EMITIDA = "EMITIDA"
    IMPAGADA = "IMPAGADA"
    PAGADA = "PAGADA"


class OwnershipTransferType(str, Enum):
    MORTIS_CAUSA_TESTAMENTO = "MORTIS_CAUSA_TESTAMENTO"
    MORTIS_CAUSA_SIN_TESTAMENTO = "MORTIS_CAUSA_SIN_TESTAMENTO"
    INTER_VIVOS = "INTER_VIVOS"
    PROVISIONAL = "PROVISIONAL"


class OwnershipTransferStatus(str, Enum):
    DRAFT = "DRAFT"
    DOCS_PENDING = "DOCS_PENDING"
    UNDER_REVIEW = "UNDER_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    CLOSED = "CLOSED"


class OwnershipPartyRole(str, Enum):
    CAUSANT = "CAUSANT"
    ANTERIOR_TITULAR = "ANTERIOR_TITULAR"
    NUEVO_TITULAR = "NUEVO_TITULAR"
    REPRESENTANTE = "REPRESENTANTE"
    OTRO = "OTRO"


class CaseDocumentStatus(str, Enum):
    MISSING = "MISSING"
    PROVIDED = "PROVIDED"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"


class BeneficiaryCloseDecision(str, Enum):
    KEEP = "KEEP"
    REPLACE = "REPLACE"


class Organization(db.Model):
    # Spec 4.1 / 4.2 - estructura organizativa (tenant)
    __tablename__ = "organization"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(db.String(120), unique=True, nullable=False)
    code: Mapped[str] = mapped_column(db.String(30), unique=True, nullable=False)
    pensionista_discount_pct: Mapped[Decimal] = mapped_column(
        db.Numeric(5, 2),
        nullable=False,
        default=Decimal("10.00"),
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    memberships = relationship("Membership", back_populates="organization")


class User(UserMixin, db.Model):
    # Spec 11.1.2 - gestión de usuarios
    __tablename__ = "user_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(db.String(255), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    password_hash: Mapped[str] = mapped_column(db.String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    memberships = relationship("Membership", back_populates="user")


class Membership(db.Model):
    # Spec 11.1.1 - roles y permisos básicos
    __tablename__ = "membership"
    __table_args__ = (UniqueConstraint("user_id", "org_id", name="uq_membership_user_org"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"), nullable=False)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False)
    role: Mapped[str] = mapped_column(db.String(30), nullable=False, default="admin")

    user = relationship("User", back_populates="memberships")
    organization = relationship("Organization", back_populates="memberships")


class Cemetery(db.Model):
    # Spec 9.0 - módulo de Cementiri (ámbito por cementerio)
    __tablename__ = "cemetery"
    __table_args__ = (UniqueConstraint("org_id", "name", name="uq_cemetery_org_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    location: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class Sepultura(db.Model):
    # Spec 9.4.1 / 9.4.2 - inventario y estado de sepulturas
    __tablename__ = "sepultura"
    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "cemetery_id",
            "bloque",
            "fila",
            "columna",
            "numero",
            name="uq_sepultura_location",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    cemetery_id: Mapped[int] = mapped_column(ForeignKey("cemetery.id"), nullable=False)
    bloque: Mapped[str] = mapped_column(db.String(20), nullable=False)
    fila: Mapped[int] = mapped_column(nullable=False)
    columna: Mapped[int] = mapped_column(nullable=False)
    via: Mapped[str] = mapped_column(db.String(20), nullable=False)
    numero: Mapped[int] = mapped_column(nullable=False)
    modalidad: Mapped[str] = mapped_column(db.String(60), nullable=False)
    estado: Mapped[SepulturaEstado] = mapped_column(
        SAEnum(SepulturaEstado, name="sepultura_estado"),
        nullable=False,
        default=SepulturaEstado.LLIURE,
    )
    tipo_bloque: Mapped[str] = mapped_column(db.String(60), nullable=False, default="")
    tipo_lapida: Mapped[str] = mapped_column(db.String(60), nullable=False, default="")
    orientacion: Mapped[str] = mapped_column(db.String(30), nullable=False, default="")
    sepultura_principal_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    principal = relationship("Sepultura", remote_side=[id], uselist=False)
    ubicaciones = relationship("SepulturaUbicacion", back_populates="sepultura", cascade="all, delete-orphan")
    contratos = relationship("DerechoFunerarioContrato", back_populates="sepultura")
    difuntos = relationship("SepulturaDifunto", back_populates="sepultura", cascade="all, delete-orphan")
    movimientos = relationship("MovimientoSepultura", back_populates="sepultura", cascade="all, delete-orphan")

    @property
    def location_label(self) -> str:
        return f"{self.bloque} / F{self.fila} C{self.columna} / N{self.numero}"


class SepulturaUbicacion(db.Model):
    # Spec 5.1 / 9.0 - ubicaciones internas por sepultura
    __tablename__ = "sepultura_ubicacion"
    __table_args__ = (UniqueConstraint("org_id", "sepultura_id", "codigo", name="uq_sepultura_ubicacion"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False)
    codigo: Mapped[str] = mapped_column(db.String(30), nullable=False)
    descripcion: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    sepultura = relationship("Sepultura", back_populates="ubicaciones")


class Person(db.Model):
    # Spec 5.1 - sujeto reutilizable (titular/beneficiario/difunto)
    __tablename__ = "person"
    __table_args__ = (UniqueConstraint("org_id", "document_id", name="uq_person_org_document"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    first_name: Mapped[str] = mapped_column(db.String(60), nullable=False)
    last_name: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    document_id: Mapped[str | None] = mapped_column(db.String(30), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


class SepulturaDifunto(db.Model):
    # Spec 9.2.1 / 9.2.2 - relación de difuntos en sepultura
    __tablename__ = "sepultura_difunto"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False)
    person_id: Mapped[int] = mapped_column(ForeignKey("person.id"), nullable=False)
    notes: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    sepultura = relationship("Sepultura", back_populates="difuntos")
    person = relationship("Person")


class DerechoFunerarioContrato(db.Model):
    # Spec 9.1.7.x - contratacion del derecho funerario
    __tablename__ = "derecho_funerario_contrato"
    __table_args__ = (
        CheckConstraint("fecha_fin >= fecha_inicio", name="ck_contract_dates"),
        Index(
            "ix_contract_org_tipo_estado_dates",
            "org_id",
            "tipo",
            "estado",
            "fecha_inicio",
            "fecha_fin",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False)
    tipo: Mapped[DerechoTipo] = mapped_column(SAEnum(DerechoTipo, name="derecho_tipo"), nullable=False)
    fecha_inicio: Mapped[date] = mapped_column(nullable=False)
    fecha_fin: Mapped[date] = mapped_column(nullable=False)
    legacy_99_years: Mapped[bool] = mapped_column(nullable=False, default=False)
    annual_fee_amount: Mapped[Decimal] = mapped_column(db.Numeric(10, 2), nullable=False, default=0)
    estado: Mapped[str] = mapped_column(db.String(20), nullable=False, default="ACTIVO")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    sepultura = relationship("Sepultura", back_populates="contratos")
    ownership_records = relationship("OwnershipRecord", back_populates="contract", cascade="all, delete-orphan")
    beneficiaries = relationship("Beneficiario", back_populates="contract", cascade="all, delete-orphan")
    tickets = relationship("TasaMantenimientoTicket", back_populates="contrato", cascade="all, delete-orphan")
    ownership_transfer_cases = relationship(
        "OwnershipTransferCase",
        back_populates="contract",
        cascade="all, delete-orphan",
    )
    contract_events = relationship("ContractEvent", back_populates="contract", cascade="all, delete-orphan")

    @property
    def duration_years(self) -> int:
        return self.fecha_fin.year - self.fecha_inicio.year

    @validates("fecha_inicio", "fecha_fin", "tipo", "legacy_99_years")
    def validate_duration_fields(self, _key, value):
        fecha_inicio = value if _key == "fecha_inicio" else self.fecha_inicio
        fecha_fin = value if _key == "fecha_fin" else self.fecha_fin
        tipo = value if _key == "tipo" else self.tipo
        legacy_99_years = value if _key == "legacy_99_years" else self.legacy_99_years
        if fecha_inicio and fecha_fin and tipo:
            years = fecha_fin.year - fecha_inicio.year
            if tipo == DerechoTipo.USO_INMEDIATO:
                max_years = 25
            elif legacy_99_years:
                max_years = 99
            else:
                max_years = 50
            if years > max_years:
                raise ValueError(f"El contrato supera el limite legal de {max_years} anos")
        return value


class OwnershipRecord(db.Model):
    # Spec 9.1.5 - titularidad actual e historica
    __tablename__ = "ownership_record"
    __table_args__ = (
        CheckConstraint("end_date IS NULL OR end_date >= start_date", name="ck_ownership_dates"),
        Index(
            "ix_ownership_record_org_contract_current",
            "org_id",
            "contract_id",
            unique=True,
            sqlite_where=text("end_date IS NULL"),
            postgresql_where=text("end_date IS NULL"),
        ),
        Index(
            "ix_ownership_record_org_contract_start",
            "org_id",
            "contract_id",
            "start_date",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False)
    person_id: Mapped[int] = mapped_column(ForeignKey("person.id"), nullable=False)
    start_date: Mapped[date] = mapped_column(nullable=False)
    end_date: Mapped[date | None] = mapped_column(nullable=True)
    is_pensioner: Mapped[bool] = mapped_column(nullable=False, default=False)
    pensioner_since_date: Mapped[date | None] = mapped_column(nullable=True)
    is_provisional: Mapped[bool] = mapped_column(nullable=False, default=False)
    provisional_until: Mapped[date | None] = mapped_column(nullable=True)
    notes: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")

    contract = relationship("DerechoFunerarioContrato", back_populates="ownership_records")
    person = relationship("Person")

    # Backward-compatible attribute aliases while old templates/services are migrated.
    @property
    def activo_desde(self) -> date:
        return self.start_date

    @property
    def contrato_id(self) -> int:
        return self.contract_id

    @property
    def activo_hasta(self) -> date | None:
        return self.end_date

    @property
    def pensionista(self) -> bool:
        return self.is_pensioner

    @property
    def pensionista_desde(self) -> date | None:
        return self.pensioner_since_date


class Beneficiario(db.Model):
    # Spec 9.1.6 - nombramiento de beneficiario
    __tablename__ = "beneficiario"
    __table_args__ = (
        Index(
            "ix_beneficiario_org_contract_current",
            "org_id",
            "contrato_id",
            unique=True,
            sqlite_where=text("activo_hasta IS NULL"),
            postgresql_where=text("activo_hasta IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contrato_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False)
    person_id: Mapped[int] = mapped_column(ForeignKey("person.id"), nullable=False)
    activo_desde: Mapped[date] = mapped_column(nullable=False)
    activo_hasta: Mapped[date | None] = mapped_column(nullable=True)

    contract = relationship("DerechoFunerarioContrato", back_populates="beneficiaries")
    person = relationship("Person")


class OwnershipTransferCase(db.Model):
    __tablename__ = "ownership_transfer_case"
    __table_args__ = (
        UniqueConstraint("org_id", "case_number", name="uq_ownership_case_org_number"),
        UniqueConstraint("org_id", "resolution_number", name="uq_ownership_case_org_resolution"),
        Index("ix_ownership_case_org_status_opened", "org_id", "status", "opened_at"),
        Index("ix_ownership_case_org_type_status", "org_id", "type", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    case_number: Mapped[str] = mapped_column(db.String(20), nullable=False)
    contract_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False, index=True)
    type: Mapped[OwnershipTransferType] = mapped_column(
        SAEnum(OwnershipTransferType, name="ownership_transfer_type"),
        nullable=False,
    )
    status: Mapped[OwnershipTransferStatus] = mapped_column(
        SAEnum(OwnershipTransferStatus, name="ownership_transfer_status"),
        nullable=False,
        default=OwnershipTransferStatus.DRAFT,
    )
    opened_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    assigned_to_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    resolution_number: Mapped[str | None] = mapped_column(db.String(20), nullable=True)
    resolution_pdf_path: Mapped[str | None] = mapped_column(db.String(255), nullable=True)
    beneficiary_close_decision: Mapped[BeneficiaryCloseDecision | None] = mapped_column(
        SAEnum(BeneficiaryCloseDecision, name="beneficiary_close_decision"),
        nullable=True,
    )
    provisional_start_date: Mapped[date | None] = mapped_column(nullable=True)
    provisional_until: Mapped[date | None] = mapped_column(nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    internal_notes: Mapped[str] = mapped_column(db.String(1000), nullable=False, default="")
    rejection_reason: Mapped[str | None] = mapped_column(db.String(500), nullable=True)

    contract = relationship("DerechoFunerarioContrato", back_populates="ownership_transfer_cases")
    created_by = relationship("User", foreign_keys=[created_by_user_id])
    assigned_to = relationship("User", foreign_keys=[assigned_to_user_id])
    parties = relationship("OwnershipTransferParty", back_populates="case", cascade="all, delete-orphan")
    documents = relationship("CaseDocument", back_populates="case", cascade="all, delete-orphan")
    publications = relationship("Publication", back_populates="case", cascade="all, delete-orphan")
    contract_events = relationship("ContractEvent", back_populates="case", cascade="all, delete-orphan")


class OwnershipTransferParty(db.Model):
    __tablename__ = "ownership_transfer_party"
    __table_args__ = (
        Index("ix_ownership_party_org_case_role", "org_id", "case_id", "role"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    case_id: Mapped[int] = mapped_column(ForeignKey("ownership_transfer_case.id"), nullable=False, index=True)
    role: Mapped[OwnershipPartyRole] = mapped_column(
        SAEnum(OwnershipPartyRole, name="ownership_party_role"),
        nullable=False,
    )
    person_id: Mapped[int] = mapped_column(ForeignKey("person.id"), nullable=False)
    percentage: Mapped[Decimal | None] = mapped_column(db.Numeric(5, 2), nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    case = relationship("OwnershipTransferCase", back_populates="parties")
    person = relationship("Person")


class CaseDocument(db.Model):
    __tablename__ = "case_document"
    __table_args__ = (
        Index("ix_case_document_org_case_type", "org_id", "case_id", "doc_type"),
        Index("ix_case_document_org_case_required_status", "org_id", "case_id", "required", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    case_id: Mapped[int] = mapped_column(ForeignKey("ownership_transfer_case.id"), nullable=False, index=True)
    doc_type: Mapped[str] = mapped_column(db.String(50), nullable=False)
    required: Mapped[bool] = mapped_column(nullable=False, default=False)
    status: Mapped[CaseDocumentStatus] = mapped_column(
        SAEnum(CaseDocumentStatus, name="case_document_status"),
        nullable=False,
        default=CaseDocumentStatus.MISSING,
    )
    file_path: Mapped[str | None] = mapped_column(db.String(255), nullable=True)
    uploaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    case = relationship("OwnershipTransferCase", back_populates="documents")
    verified_by = relationship("User")


class Publication(db.Model):
    __tablename__ = "publication"
    __table_args__ = (
        Index("ix_publication_org_case_published", "org_id", "case_id", "published_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    case_id: Mapped[int] = mapped_column(ForeignKey("ownership_transfer_case.id"), nullable=False, index=True)
    published_at: Mapped[date] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(db.String(50), nullable=False)
    reference_text: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    case = relationship("OwnershipTransferCase", back_populates="publications")


class ContractEvent(db.Model):
    __tablename__ = "contract_event"
    __table_args__ = (
        Index("ix_contract_event_org_contract_at", "org_id", "contract_id", "event_at"),
        Index("ix_contract_event_org_case_at", "org_id", "case_id", "event_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contract_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False)
    case_id: Mapped[int | None] = mapped_column(ForeignKey("ownership_transfer_case.id"), nullable=True)
    event_type: Mapped[str] = mapped_column(db.String(50), nullable=False)
    event_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    details: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)

    contract = relationship("DerechoFunerarioContrato", back_populates="contract_events")
    case = relationship("OwnershipTransferCase", back_populates="contract_events")
    user = relationship("User")


class MovimientoSepultura(db.Model):
    # Spec 9.4.5 - consulta de movimientos
    __tablename__ = "movimiento_sepultura"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False, index=True)
    tipo: Mapped[MovimientoTipo] = mapped_column(SAEnum(MovimientoTipo, name="movimiento_tipo"), nullable=False)
    fecha: Mapped[datetime] = mapped_column(default=utcnow, nullable=False, index=True)
    detalle: Mapped[str] = mapped_column(db.String(255), nullable=False)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)

    sepultura = relationship("Sepultura", back_populates="movimientos")
    user = relationship("User")


class Invoice(db.Model):
    # Spec 5.2.5.2.2 / 9.1.3 - facturación de tasas
    __tablename__ = "invoice"
    __table_args__ = (UniqueConstraint("org_id", "numero", name="uq_invoice_org_number"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contrato_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False)
    numero: Mapped[str] = mapped_column(db.String(40), nullable=False)
    estado: Mapped[InvoiceEstado] = mapped_column(
        SAEnum(InvoiceEstado, name="invoice_estado"),
        nullable=False,
        default=InvoiceEstado.BORRADOR,
    )
    total_amount: Mapped[Decimal] = mapped_column(db.Numeric(10, 2), nullable=False, default=0)
    issued_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    payments = relationship("Payment", back_populates="invoice", cascade="all, delete-orphan")


class Payment(db.Model):
    # Spec 9.1.3 - cobro y recibo
    __tablename__ = "payment"
    __table_args__ = (UniqueConstraint("org_id", "receipt_number", name="uq_payment_org_receipt"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoice.id"), nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True, index=True)
    amount: Mapped[Decimal] = mapped_column(db.Numeric(10, 2), nullable=False)
    method: Mapped[str] = mapped_column(db.String(20), nullable=False, default="EFECTIVO")
    receipt_number: Mapped[str] = mapped_column(db.String(40), nullable=False)
    paid_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    invoice = relationship("Invoice", back_populates="payments")
    user = relationship("User")


class TasaMantenimientoTicket(db.Model):
    # Spec 5.2.5.2.2 / 9.1.3 - tiquets anuales de mantenimiento
    __tablename__ = "tasa_mantenimiento_ticket"
    __table_args__ = (UniqueConstraint("org_id", "contrato_id", "anio", name="uq_ticket_contract_year"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contrato_id: Mapped[int] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=False)
    invoice_id: Mapped[int | None] = mapped_column(ForeignKey("invoice.id"), nullable=True)
    anio: Mapped[int] = mapped_column(nullable=False)
    importe: Mapped[Decimal] = mapped_column(db.Numeric(10, 2), nullable=False)
    descuento_tipo: Mapped[TicketDescuentoTipo] = mapped_column(
        SAEnum(TicketDescuentoTipo, name="ticket_descuento_tipo"),
        nullable=False,
        default=TicketDescuentoTipo.NONE,
    )
    estado: Mapped[TicketEstado] = mapped_column(
        SAEnum(TicketEstado, name="ticket_estado"),
        nullable=False,
        default=TicketEstado.PENDIENTE,
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    contrato = relationship("DerechoFunerarioContrato", back_populates="tickets")
    invoice = relationship("Invoice")


class Expediente(db.Model):
    # Spec 9.1.1 / 9.1.2 / 9.1.8.4 - preparado para MVP+
    __tablename__ = "expediente"
    __table_args__ = (UniqueConstraint("org_id", "numero", name="uq_expediente_org_number"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    numero: Mapped[str] = mapped_column(db.String(40), nullable=False)
    tipo: Mapped[str] = mapped_column(db.String(40), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True)
    difunto_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class OrdenTrabajo(db.Model):
    # Spec 9.2 / 9.3 / 9.1.10 - preparado para MVP+
    __tablename__ = "orden_trabajo"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    expediente_id: Mapped[int | None] = mapped_column(ForeignKey("expediente.id"), nullable=True)
    titulo: Mapped[str] = mapped_column(db.String(120), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class LapidaStock(db.Model):
    # Spec 9.2.6 / 9.1.9 - preparado para MVP+
    __tablename__ = "lapida_stock"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    codigo: Mapped[str] = mapped_column(db.String(40), nullable=False)
    descripcion: Mapped[str] = mapped_column(db.String(120), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class InscripcionLateral(db.Model):
    # Spec 9.2.7 - estado de inscripciones laterales
    __tablename__ = "inscripcion_lateral"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False)
    texto: Mapped[str] = mapped_column(db.String(255), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False, default="PENDIENTE_GRABAR")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


@event.listens_for(DerechoFunerarioContrato, "after_insert")
def contract_after_insert(_mapper, connection, target: DerechoFunerarioContrato) -> None:
    # Spec 9.4.2 - estado Ocupada provocado por contrato
    connection.execute(
        Sepultura.__table__.update()
        .where(Sepultura.id == target.sepultura_id)
        .values(estado=SepulturaEstado.OCUPADA)
    )
    connection.execute(
        MovimientoSepultura.__table__.insert().values(
            org_id=target.org_id,
            sepultura_id=target.sepultura_id,
            tipo=MovimientoTipo.CONTRATO,
            fecha=utcnow(),
            detalle=f"Contrato {target.tipo.value} {target.fecha_inicio} - {target.fecha_fin}",
            user_id=None,
        )
    )


@event.listens_for(Sepultura, "after_update")
def sepultura_after_update(_mapper, connection, target: Sepultura) -> None:
    # Spec 9.4.2 - trazabilidad de cambios de estado
    state = inspect(target)
    if state.attrs.estado.history.has_changes():
        connection.execute(
            MovimientoSepultura.__table__.insert().values(
                org_id=target.org_id,
                sepultura_id=target.id,
                tipo=MovimientoTipo.CAMBIO_ESTADO,
                fecha=utcnow(),
                detalle=f"Cambio de estado a {target.estado.value}",
                user_id=None,
            )
        )


@event.listens_for(TasaMantenimientoTicket, "after_update")
def ticket_after_update(_mapper, connection, target: TasaMantenimientoTicket) -> None:
    # Spec 9.1.3 - trazabilidad en cobro/facturación de tasas
    state = inspect(target)
    if state.attrs.estado.history.has_changes():
        contrato = connection.execute(
            DerechoFunerarioContrato.__table__.select().where(DerechoFunerarioContrato.id == target.contrato_id)
        ).mappings().first()
        if contrato:
            connection.execute(
                MovimientoSepultura.__table__.insert().values(
                    org_id=target.org_id,
                    sepultura_id=contrato["sepultura_id"],
                    tipo=MovimientoTipo.TASAS,
                    fecha=utcnow(),
                    detalle=f"Tiquet {target.anio} -> {target.estado.value}",
                    user_id=None,
                )
            )


def seed_demo_data(session) -> None:
    org = Organization(name="SMSFT Demo", code="SMSFT", pensionista_discount_pct=Decimal("10.00"))
    session.add(org)
    session.flush()

    admin = User(
        email="admin@smsft.local",
        full_name="Admin Cementerio",
        password_hash=generate_password_hash("admin123"),
    )
    operario = User(
        email="operario@smsft.local",
        full_name="Operario Cementerio",
        password_hash=generate_password_hash("operario123"),
    )
    session.add_all([admin, operario])
    session.flush()

    session.add_all(
        [
            Membership(user_id=admin.id, org_id=org.id, role="admin"),
            Membership(user_id=operario.id, org_id=org.id, role="operator"),
        ]
    )

    cemetery = Cemetery(org_id=org.id, name="Cementiri Municipal", location="Terrassa")
    session.add(cemetery)
    session.flush()

    sep_1 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-12",
        fila=4,
        columna=18,
        via="V-3",
        numero=127,
        modalidad="Nínxol",
        estado=SepulturaEstado.DISPONIBLE,
        tipo_bloque="Nínxols",
        tipo_lapida="Resina fenòlica",
        orientacion="Nord",
    )
    sep_2 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-12",
        fila=4,
        columna=19,
        via="V-3",
        numero=128,
        modalidad="Nínxol",
        estado=SepulturaEstado.DISPONIBLE,
        tipo_bloque="Nínxols",
        tipo_lapida="Resina fenòlica",
        orientacion="Nord",
    )
    sep_3 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-07",
        fila=1,
        columna=2,
        via="V-1",
        numero=9,
        modalidad="Fossa comú",
        estado=SepulturaEstado.PROPIA,
        tipo_bloque="Fossa",
        tipo_lapida="Sense làpida",
        orientacion="Sud",
    )
    sep_4 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-15",
        fila=9,
        columna=11,
        via="V-5",
        numero=332,
        modalidad="Panteó",
        estado=SepulturaEstado.INACTIVA,
        tipo_bloque="Panteons",
        tipo_lapida="Marbre",
        orientacion="Oest",
    )
    sep_5 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-20",
        fila=2,
        columna=7,
        via="V-4",
        numero=210,
        modalidad="Nínxol",
        estado=SepulturaEstado.DISPONIBLE,
        tipo_bloque="Nínxols",
        tipo_lapida="Resina fenòlica",
        orientacion="Est",
    )
    sep_6 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-22",
        fila=1,
        columna=1,
        via="V-6",
        numero=401,
        modalidad="NÃ­nxol",
        estado=SepulturaEstado.DISPONIBLE,
        tipo_bloque="NÃ­nxols",
        tipo_lapida="Resina fenÃ²lica",
        orientacion="Nord",
    )
    sep_7 = Sepultura(
        org_id=org.id,
        cemetery_id=cemetery.id,
        bloque="B-30",
        fila=2,
        columna=3,
        via="V-7",
        numero=510,
        modalidad="NÃ­nxol",
        estado=SepulturaEstado.DISPONIBLE,
        tipo_bloque="NÃ­nxols",
        tipo_lapida="Resina fenÃ²lica",
        orientacion="Est",
    )
    session.add_all([sep_1, sep_2, sep_3, sep_4, sep_5, sep_6, sep_7])
    session.flush()

    titular_1 = Person(org_id=org.id, first_name="Marta", last_name="Soler", document_id="11111111A")
    titular_2 = Person(org_id=org.id, first_name="Joan", last_name="Riera", document_id="22222222B")
    titular_3 = Person(org_id=org.id, first_name="Pere", last_name="Casals", document_id="44444444D")
    difunto_1 = Person(org_id=org.id, first_name="Antoni", last_name="Ferrer", document_id="33333333C")
    session.add_all([titular_1, titular_2, titular_3, difunto_1])
    session.flush()

    contrato_1 = DerechoFunerarioContrato(
        org_id=org.id,
        sepultura_id=sep_1.id,
        tipo=DerechoTipo.CONCESION,
        fecha_inicio=date(2012, 1, 1),
        fecha_fin=date(2037, 1, 1),
        annual_fee_amount=Decimal("45.00"),
        estado="ACTIVO",
    )
    contrato_2 = DerechoFunerarioContrato(
        org_id=org.id,
        sepultura_id=sep_5.id,
        tipo=DerechoTipo.CONCESION,
        fecha_inicio=date(2018, 1, 1),
        fecha_fin=date(2043, 1, 1),
        annual_fee_amount=Decimal("50.00"),
        estado="ACTIVO",
    )
    contrato_3 = DerechoFunerarioContrato(
        org_id=org.id,
        sepultura_id=sep_2.id,
        tipo=DerechoTipo.USO_INMEDIATO,
        fecha_inicio=date(2024, 1, 1),
        fecha_fin=date(2030, 1, 1),
        annual_fee_amount=Decimal("30.00"),
        estado="ACTIVO",
    )
    contrato_legacy = DerechoFunerarioContrato(
        org_id=org.id,
        sepultura_id=sep_6.id,
        tipo=DerechoTipo.CONCESION,
        fecha_inicio=date(1980, 1, 1),
        legacy_99_years=True,
        fecha_fin=date(2079, 1, 1),
        annual_fee_amount=Decimal("35.00"),
        estado="ACTIVO",
    )
    session.add_all([contrato_1, contrato_2, contrato_3, contrato_legacy])
    session.flush()

    session.add_all(
        [
            OwnershipRecord(
                org_id=org.id,
                contract_id=contrato_1.id,
                person_id=titular_1.id,
                start_date=date(2012, 1, 1),
                is_pensioner=True,
                pensioner_since_date=date(2025, 1, 1),
            ),
            OwnershipRecord(
                org_id=org.id,
                contract_id=contrato_2.id,
                person_id=titular_2.id,
                start_date=date(2018, 1, 1),
                is_pensioner=False,
            ),
            OwnershipRecord(
                org_id=org.id,
                contract_id=contrato_3.id,
                person_id=titular_2.id,
                start_date=date(2024, 1, 1),
                is_pensioner=False,
            ),
            OwnershipRecord(
                org_id=org.id,
                contract_id=contrato_legacy.id,
                person_id=titular_3.id,
                start_date=date(1980, 1, 1),
                is_pensioner=False,
            ),
            Beneficiario(
                org_id=org.id,
                contrato_id=contrato_2.id,
                person_id=titular_1.id,
                activo_desde=date(2024, 1, 1),
            ),
        ]
    )

    session.add(SepulturaDifunto(org_id=org.id, sepultura_id=sep_1.id, person_id=difunto_1.id, notes="Cadàver"))

    ticket_2023 = TasaMantenimientoTicket(
        org_id=org.id,
        contrato_id=contrato_1.id,
        anio=2023,
        importe=Decimal("45.00"),
        descuento_tipo=TicketDescuentoTipo.NONE,
        estado=TicketEstado.PENDIENTE,
    )
    ticket_2024 = TasaMantenimientoTicket(
        org_id=org.id,
        contrato_id=contrato_1.id,
        anio=2024,
        importe=Decimal("45.00"),
        descuento_tipo=TicketDescuentoTipo.NONE,
        estado=TicketEstado.PENDIENTE,
    )
    ticket_2025 = TasaMantenimientoTicket(
        org_id=org.id,
        contrato_id=contrato_1.id,
        anio=2025,
        importe=Decimal("40.50"),
        descuento_tipo=TicketDescuentoTipo.PENSIONISTA,
        estado=TicketEstado.PENDIENTE,
    )
    ticket_2026 = TasaMantenimientoTicket(
        org_id=org.id,
        contrato_id=contrato_1.id,
        anio=2026,
        importe=Decimal("40.50"),
        descuento_tipo=TicketDescuentoTipo.PENSIONISTA,
        estado=TicketEstado.PENDIENTE,
    )

    invoice_old = Invoice(
        org_id=org.id,
        contrato_id=contrato_2.id,
        sepultura_id=sep_5.id,
        numero="F-CEM-2025-0001",
        estado=InvoiceEstado.IMPAGADA,
        total_amount=Decimal("50.00"),
        issued_at=utcnow(),
    )
    session.add(invoice_old)
    session.flush()

    ticket_impagado = TasaMantenimientoTicket(
        org_id=org.id,
        contrato_id=contrato_2.id,
        anio=2025,
        importe=Decimal("50.00"),
        descuento_tipo=TicketDescuentoTipo.NONE,
        estado=TicketEstado.FACTURADO,
        invoice_id=invoice_old.id,
    )

    session.add_all([ticket_2023, ticket_2024, ticket_2025, ticket_2026, ticket_impagado])

    session.add_all(
        [
            MovimientoSepultura(
                org_id=org.id,
                sepultura_id=sep_1.id,
                tipo=MovimientoTipo.INHUMACION,
                detalle="Antoni Ferrer (Cadàver)",
                user_id=operario.id,
            ),
            MovimientoSepultura(
                org_id=org.id,
                sepultura_id=sep_1.id,
                tipo=MovimientoTipo.LAPIDA,
                detalle="Colocación lápida resina",
                user_id=operario.id,
            ),
            MovimientoSepultura(
                org_id=org.id,
                sepultura_id=sep_1.id,
                tipo=MovimientoTipo.TASAS,
                detalle="Tiquet 2025 generado",
                user_id=None,
            ),
        ]
    )

    session.add_all(
        [
            Expediente(
                org_id=org.id,
                numero="C-2026-0012",
                tipo="INHUMACION",
                estado="TRAMITACION",
                sepultura_id=sep_1.id,
                difunto_id=difunto_1.id,
            ),
            Expediente(
                org_id=org.id,
                numero="C-2026-0011",
                tipo="EXHUMACION",
                estado="PEND_AUTORIZACION",
                sepultura_id=sep_5.id,
            ),
            Expediente(
                org_id=org.id,
                numero="C-2026-0010",
                tipo="INHUMACION",
                estado="OT_EN_CURSO",
                sepultura_id=sep_1.id,
            ),
            OrdenTrabajo(org_id=org.id, titulo="Preparar lápida", estado="PENDIENTE"),
            OrdenTrabajo(org_id=org.id, titulo="Revisión bloque B-12", estado="PENDIENTE"),
            OrdenTrabajo(org_id=org.id, titulo="Limpieza pasillo V-3", estado="EN_CURSO"),
            InscripcionLateral(
                org_id=org.id,
                sepultura_id=sep_1.id,
                texto="Familia Ferrer",
                estado="PENDIENTE_COLOCAR",
            ),
            InscripcionLateral(
                org_id=org.id,
                sepultura_id=sep_5.id,
                texto="Familia Riera",
                estado="PENDIENTE_NOTIFICAR",
            ),
            InscripcionLateral(
                org_id=org.id,
                sepultura_id=sep_1.id,
                texto="Record etern",
                estado="PENDIENTE_NOTIFICAR",
            ),
        ]
    )

    successor_1 = Person(org_id=org.id, first_name="Carla", last_name="Mora", document_id="88888888H")
    successor_2 = Person(org_id=org.id, first_name="Sonia", last_name="Pons", document_id="99999999J")
    successor_3 = Person(org_id=org.id, first_name="Marc", last_name="Vila", document_id="10101010K")
    session.add_all([successor_1, successor_2, successor_3])
    session.flush()

    case_1 = OwnershipTransferCase(
        org_id=org.id,
        case_number="TR-2026-0001",
        contract_id=contrato_1.id,
        type=OwnershipTransferType.INTER_VIVOS,
        status=OwnershipTransferStatus.DRAFT,
        created_by_user_id=admin.id,
        assigned_to_user_id=operario.id,
        notes="Caso demo inter-vivos",
    )
    case_2 = OwnershipTransferCase(
        org_id=org.id,
        case_number="TR-2026-0002",
        contract_id=contrato_2.id,
        type=OwnershipTransferType.MORTIS_CAUSA_TESTAMENTO,
        status=OwnershipTransferStatus.DOCS_PENDING,
        created_by_user_id=admin.id,
        notes="Caso demo mortis-causa testamento",
    )
    case_3 = OwnershipTransferCase(
        org_id=org.id,
        case_number="TR-2026-0003",
        contract_id=contrato_3.id,
        type=OwnershipTransferType.PROVISIONAL,
        status=OwnershipTransferStatus.APPROVED,
        created_by_user_id=admin.id,
        provisional_start_date=date(2026, 1, 1),
        provisional_until=date(2036, 1, 1),
        resolution_number="RES-2026-0001",
        resolution_pdf_path=None,
        notes="Caso demo provisional",
    )
    case_4 = OwnershipTransferCase(
        org_id=org.id,
        case_number="TR-2026-0004",
        contract_id=contrato_legacy.id,
        type=OwnershipTransferType.MORTIS_CAUSA_SIN_TESTAMENTO,
        status=OwnershipTransferStatus.REJECTED,
        created_by_user_id=admin.id,
        rejection_reason="Documentacion incompleta",
        notes="Caso demo mortis-causa sin testamento",
    )
    session.add_all([case_1, case_2, case_3, case_4])
    session.flush()
    case_3.resolution_pdf_path = (
        f"storage/cemetery/ownership_cases/{org.id}/{case_3.id}/resolucion-{case_3.resolution_number}.pdf"
    )

    session.add_all(
        [
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_1.id,
                role=OwnershipPartyRole.ANTERIOR_TITULAR,
                person_id=titular_1.id,
            ),
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_1.id,
                role=OwnershipPartyRole.NUEVO_TITULAR,
                person_id=successor_1.id,
            ),
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_2.id,
                role=OwnershipPartyRole.ANTERIOR_TITULAR,
                person_id=titular_2.id,
            ),
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_2.id,
                role=OwnershipPartyRole.NUEVO_TITULAR,
                person_id=successor_3.id,
            ),
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_3.id,
                role=OwnershipPartyRole.ANTERIOR_TITULAR,
                person_id=titular_2.id,
            ),
            OwnershipTransferParty(
                org_id=org.id,
                case_id=case_3.id,
                role=OwnershipPartyRole.NUEVO_TITULAR,
                person_id=successor_2.id,
            ),
        ]
    )

    for case in [case_1, case_2, case_3, case_4]:
        checklist = {
            OwnershipTransferType.MORTIS_CAUSA_TESTAMENTO: [
                ("CERT_DEFUNCION", True),
                ("TITULO_SEPULTURA", True),
                ("SOLICITUD_CAMBIO_TITULARIDAD", True),
                ("CERT_ULTIMAS_VOLUNTADES", True),
                ("TESTAMENTO_O_ACEPTACION_HERENCIA", True),
                ("CESION_DERECHOS", False),
                ("SOLICITUD_BENEFICIARIO", False),
                ("DNI_NUEVO_BENEFICIARIO", False),
            ],
            OwnershipTransferType.MORTIS_CAUSA_SIN_TESTAMENTO: [
                ("CERT_DEFUNCION", True),
                ("TITULO_SEPULTURA", True),
                ("SOLICITUD_CAMBIO_TITULARIDAD", True),
                ("CERT_ULTIMAS_VOLUNTADES", True),
                ("LIBRO_FAMILIA_O_TESTIGOS", False),
                ("CESION_DERECHOS", False),
                ("SOLICITUD_BENEFICIARIO", False),
                ("DNI_NUEVO_BENEFICIARIO", False),
            ],
            OwnershipTransferType.INTER_VIVOS: [
                ("SOLICITUD_CAMBIO_TITULARIDAD", True),
                ("TITULO_SEPULTURA", True),
                ("DNI_TITULAR_ACTUAL", True),
                ("DNI_NUEVO_TITULAR", True),
                ("SOLICITUD_BENEFICIARIO", False),
                ("DNI_NUEVO_BENEFICIARIO", False),
            ],
            OwnershipTransferType.PROVISIONAL: [
                ("SOLICITUD_CAMBIO_TITULARIDAD", True),
                ("ACEPTACION_SMSFT", True),
                ("PUBLICACION_BOP", True),
                ("PUBLICACION_DIARIO", True),
                ("SOLICITUD_BENEFICIARIO", False),
                ("DNI_NUEVO_BENEFICIARIO", False),
            ],
        }[case.type]
        for doc_type, required in checklist:
            status = CaseDocumentStatus.MISSING
            if case.status == OwnershipTransferStatus.APPROVED and required:
                status = CaseDocumentStatus.VERIFIED
            session.add(
                CaseDocument(
                    org_id=org.id,
                    case_id=case.id,
                    doc_type=doc_type,
                    required=required,
                    status=status,
                    uploaded_at=utcnow() if status != CaseDocumentStatus.MISSING else None,
                    verified_at=utcnow() if status == CaseDocumentStatus.VERIFIED else None,
                    verified_by_user_id=admin.id if status == CaseDocumentStatus.VERIFIED else None,
                )
            )

    session.add_all(
        [
            Publication(
                org_id=org.id,
                case_id=case_3.id,
                published_at=date(2026, 2, 1),
                channel="BOP",
                reference_text="BOP-2026-100",
            ),
            Publication(
                org_id=org.id,
                case_id=case_3.id,
                published_at=date(2026, 2, 5),
                channel="DIARIO",
                reference_text="Diari Terrassa 05/02/2026",
            ),
            ContractEvent(
                org_id=org.id,
                contract_id=contrato_1.id,
                case_id=case_1.id,
                event_type="INICIO_TRANSMISION",
                details="Caso demo TR-2026-0001",
                user_id=admin.id,
            ),
        ]
    )
    session.commit()
