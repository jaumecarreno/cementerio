from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

from flask_login import UserMixin
from sqlalchemy import CheckConstraint, Enum as SAEnum, ForeignKey, Index, UniqueConstraint, event, inspect, text
from sqlalchemy.orm import Mapped, mapped_column, relationship, synonym, validates
from werkzeug.security import generate_password_hash

from app.core.demo_people import is_generic_demo_name
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


class MovimientoTipo(str, Enum):
    INHUMACION = "INHUMACION"
    EXHUMACION = "EXHUMACION"
    TRASLADO_CORTO = "TRASLADO_CORTO"
    TRASLADO_LARGO = "TRASLADO_LARGO"
    RESCATE = "RESCATE"
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
    ALTA_EXPEDIENTE = "ALTA_EXPEDIENTE"
    CAMBIO_ESTADO_EXPEDIENTE = "CAMBIO_ESTADO_EXPEDIENTE"
    OT_EXPEDIENTE = "OT_EXPEDIENTE"
    BENEFICIARIO = "BENEFICIARIO"
    PENSIONISTA = "PENSIONISTA"


class WorkOrderCategory(str, Enum):
    FUNERARIA = "FUNERARIA"
    MANTENIMIENTO = "MANTENIMIENTO"
    INCIDENCIA = "INCIDENCIA"
    ADMINISTRATIVA = "ADMINISTRATIVA"


class WorkOrderPriority(str, Enum):
    BAJA = "BAJA"
    MEDIA = "MEDIA"
    ALTA = "ALTA"
    URGENTE = "URGENTE"


class WorkOrderStatus(str, Enum):
    BORRADOR = "BORRADOR"
    PENDIENTE_PLANIFICACION = "PENDIENTE_PLANIFICACION"
    PLANIFICADA = "PLANIFICADA"
    ASIGNADA = "ASIGNADA"
    EN_CURSO = "EN_CURSO"
    BLOQUEADA = "BLOQUEADA"
    EN_VALIDACION = "EN_VALIDACION"
    COMPLETADA = "COMPLETADA"
    CANCELADA = "CANCELADA"


class OperationType(str, Enum):
    INHUMACION = "INHUMACION"
    EXHUMACION = "EXHUMACION"
    TRASLADO_CORTO = "TRASLADO_CORTO"
    TRASLADO_LARGO = "TRASLADO_LARGO"
    RESCATE = "RESCATE"


class OperationStatus(str, Enum):
    BORRADOR = "BORRADOR"
    DOCS_PENDIENTES = "DOCS_PENDIENTES"
    PROGRAMADA = "PROGRAMADA"
    EN_EJECUCION = "EN_EJECUCION"
    EN_VALIDACION = "EN_VALIDACION"
    CERRADA = "CERRADA"
    CANCELADA = "CANCELADA"


class OperationPermitStatus(str, Enum):
    MISSING = "MISSING"
    PROVIDED = "PROVIDED"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"


class WorkOrderAreaType(str, Enum):
    SECTOR = "SECTOR"
    BLOQUE = "BLOQUE"
    VIAL = "VIAL"
    GENERAL = "GENERAL"


class WorkOrderDependencyType(str, Enum):
    FINISH_TO_START = "FINISH_TO_START"


class BillingDocumentType(str, Enum):
    INVOICE = "INVOICE"
    CREDIT_NOTE = "CREDIT_NOTE"


class BillingDocumentStatus(str, Enum):
    DRAFT = "DRAFT"
    ISSUED = "ISSUED"
    PARTIALLY_PAID = "PARTIALLY_PAID"
    PAID = "PAID"
    CANCELLED = "CANCELLED"


class PaymentMethod(str, Enum):
    EFECTIVO = "EFECTIVO"
    TARJETA = "TARJETA"
    TRANSFERENCIA = "TRANSFERENCIA"
    BIZUM = "BIZUM"


class FiscalSubmissionStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    RETRYING = "RETRYING"


class OwnershipTransferType(str, Enum):
    MORTIS_CAUSA_TESTAMENTO = "MORTIS_CAUSA_TESTAMENTO"
    MORTIS_CAUSA_SIN_TESTAMENTO = "MORTIS_CAUSA_SIN_TESTAMENTO"
    MORTIS_CAUSA_CON_BENEFICIARIO = "MORTIS_CAUSA_CON_BENEFICIARIO"
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


OWNERSHIP_CASE_CHECKLIST: dict[OwnershipTransferType, list[tuple[str, bool]]] = {
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
        ("LIBRO_FAMILIA_O_TESTIGOS", True),
        ("CESION_DERECHOS", False),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
    OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO: [
        ("CERT_DEFUNCION", True),
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("TITULO_SEPULTURA", True),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
    OwnershipTransferType.INTER_VIVOS: [
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("TITULO_SEPULTURA", True),
        ("DNI_TITULAR_ACTUAL", True),
        ("DNI_NUEVO_TITULAR", True),
        ("LIBRO_FAMILIA_O_TESTIGOS", True),
        ("ACREDITACION_PARENTESCO_2_GRADO", True),
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
}


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
    municipality: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
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
    postit: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    notas: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
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
    __table_args__ = (
        Index(
            "ix_person_org_dni_nif_not_null",
            "org_id",
            "dni_nif",
            unique=True,
            sqlite_where=text("dni_nif IS NOT NULL"),
            postgresql_where=text("dni_nif IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    first_name: Mapped[str] = mapped_column(db.String(60), nullable=False)
    last_name: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    dni_nif: Mapped[str | None] = mapped_column(db.String(30), nullable=True)
    telefono: Mapped[str] = mapped_column(db.String(40), nullable=False, default="")
    telefono2: Mapped[str] = mapped_column(db.String(40), nullable=False, default="")
    email: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    email2: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    direccion: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    direccion_linea: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    codigo_postal: Mapped[str] = mapped_column(db.String(20), nullable=False, default="")
    poblacion: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    provincia: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    pais: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    notas: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    # Backward-compatible alias used by older services/templates/tests.
    document_id = synonym("dni_nif")

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    @property
    def formatted_address(self) -> str:
        parts: list[str] = []
        street = (self.direccion_linea or "").strip()
        if street:
            parts.append(street)
        locality = " ".join(
            [part for part in [(self.codigo_postal or "").strip(), (self.poblacion or "").strip()] if part]
        ).strip()
        if locality:
            parts.append(locality)
        if (self.provincia or "").strip():
            parts.append((self.provincia or "").strip())
        if (self.pais or "").strip():
            parts.append((self.pais or "").strip())
        if parts:
            return ", ".join(parts)
        return (self.direccion or "").strip()


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


class ActivityLog(db.Model):
    __tablename__ = "activity_log"
    __table_args__ = (
        Index("ix_activity_log_org_created_at", "org_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True, index=True)
    action_type: Mapped[str] = mapped_column(db.String(60), nullable=False)
    details: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True, index=True)

    sepultura = relationship("Sepultura")
    user = relationship("User")


class ReportSchedule(db.Model):
    __tablename__ = "report_schedule"
    __table_args__ = (
        Index("ix_report_schedule_org_active_cadence", "org_id", "active", "cadence"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    report_key: Mapped[str] = mapped_column(db.String(80), nullable=False)
    cadence: Mapped[str] = mapped_column(db.String(20), nullable=False)
    day_of_week: Mapped[int | None] = mapped_column(nullable=True)
    day_of_month: Mapped[int | None] = mapped_column(nullable=True)
    run_time: Mapped[str] = mapped_column(db.String(5), nullable=False, default="07:00")
    timezone: Mapped[str] = mapped_column(db.String(64), nullable=False, default="Europe/Madrid")
    recipients: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
    filters_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    formats: Mapped[str] = mapped_column(db.String(40), nullable=False, default="CSV")
    active: Mapped[bool] = mapped_column(nullable=False, default=True)
    last_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    created_by = relationship("User")
    deliveries = relationship("ReportDeliveryLog", back_populates="schedule", cascade="all, delete-orphan")


class ReportDeliveryLog(db.Model):
    __tablename__ = "report_delivery_log"
    __table_args__ = (
        Index("ix_report_delivery_schedule_run", "schedule_id", "run_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    schedule_id: Mapped[int] = mapped_column(ForeignKey("report_schedule.id"), nullable=False, index=True)
    run_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    status: Mapped[str] = mapped_column(db.String(20), nullable=False, default="SUCCESS")
    rows_count: Mapped[int] = mapped_column(nullable=False, default=0)
    artifacts_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="[]")
    error: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    schedule = relationship("ReportSchedule", back_populates="deliveries")


class OperationCase(db.Model):
    __tablename__ = "operation_case"
    __table_args__ = (
        UniqueConstraint("org_id", "code", name="uq_operation_case_org_code"),
        Index("ix_operation_case_org_status_scheduled", "org_id", "status", "scheduled_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(db.String(40), nullable=False)
    type: Mapped[OperationType] = mapped_column(
        SAEnum(OperationType, name="operation_type"),
        nullable=False,
    )
    status: Mapped[OperationStatus] = mapped_column(
        SAEnum(OperationStatus, name="operation_status"),
        nullable=False,
        default=OperationStatus.BORRADOR,
    )
    source_sepultura_id: Mapped[int] = mapped_column(ForeignKey("sepultura.id"), nullable=False, index=True)
    target_sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True, index=True)
    deceased_person_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True, index=True)
    declarant_person_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True, index=True)
    holder_person_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True, index=True)
    beneficiary_person_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True, index=True)
    contract_id: Mapped[int | None] = mapped_column(
        ForeignKey("derecho_funerario_contrato.id"),
        nullable=True,
        index=True,
    )
    concession_start_date: Mapped[date | None] = mapped_column(nullable=True)
    concession_end_date: Mapped[date | None] = mapped_column(nullable=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    destination_cemetery_id: Mapped[int | None] = mapped_column(ForeignKey("cemetery.id"), nullable=True, index=True)
    destination_name: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    destination_municipality: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    destination_region: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    destination_country: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    cross_border: Mapped[bool] = mapped_column(nullable=False, default=False)
    notes: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    managed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False, index=True)

    source_sepultura = relationship("Sepultura", foreign_keys=[source_sepultura_id])
    target_sepultura = relationship("Sepultura", foreign_keys=[target_sepultura_id])
    deceased_person = relationship("Person", foreign_keys=[deceased_person_id])
    declarant_person = relationship("Person", foreign_keys=[declarant_person_id])
    holder_person = relationship("Person", foreign_keys=[holder_person_id])
    beneficiary_person = relationship("Person", foreign_keys=[beneficiary_person_id])
    contract = relationship("DerechoFunerarioContrato", foreign_keys=[contract_id])
    destination_cemetery = relationship("Cemetery", foreign_keys=[destination_cemetery_id])
    created_by = relationship("User", foreign_keys=[created_by_user_id])
    managed_by = relationship("User", foreign_keys=[managed_by_user_id])
    permits = relationship("OperationPermit", back_populates="operation_case", cascade="all, delete-orphan")
    documents = relationship("OperationDocument", back_populates="operation_case", cascade="all, delete-orphan")
    status_logs = relationship("OperationStatusLog", back_populates="operation_case", cascade="all, delete-orphan")
    work_orders = relationship("WorkOrder", back_populates="operation_case")

    @property
    def concession_duration_years(self) -> int | None:
        if self.concession_start_date and self.concession_end_date:
            return self.concession_end_date.year - self.concession_start_date.year
        return None


class OperationPermit(db.Model):
    __tablename__ = "operation_permit"
    __table_args__ = (
        Index("ix_operation_permit_case_type", "operation_case_id", "permit_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    operation_case_id: Mapped[int] = mapped_column(ForeignKey("operation_case.id"), nullable=False, index=True)
    permit_type: Mapped[str] = mapped_column(db.String(80), nullable=False)
    required: Mapped[bool] = mapped_column(nullable=False, default=True)
    status: Mapped[OperationPermitStatus] = mapped_column(
        SAEnum(OperationPermitStatus, name="operation_permit_status"),
        nullable=False,
        default=OperationPermitStatus.MISSING,
    )
    reference_number: Mapped[str] = mapped_column(db.String(80), nullable=False, default="")
    issued_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    operation_case = relationship("OperationCase", back_populates="permits")
    verified_by = relationship("User")


class OperationDocument(db.Model):
    __tablename__ = "operation_document"
    __table_args__ = (
        Index("ix_operation_document_case_type", "operation_case_id", "doc_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    operation_case_id: Mapped[int] = mapped_column(ForeignKey("operation_case.id"), nullable=False, index=True)
    doc_type: Mapped[str] = mapped_column(db.String(80), nullable=False)
    file_path: Mapped[str | None] = mapped_column(db.String(255), nullable=True)
    required: Mapped[bool] = mapped_column(nullable=False, default=False)
    status: Mapped[OperationPermitStatus] = mapped_column(
        SAEnum(OperationPermitStatus, name="operation_permit_status"),
        nullable=False,
        default=OperationPermitStatus.MISSING,
    )
    uploaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(nullable=True)
    verified_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    operation_case = relationship("OperationCase", back_populates="documents")
    verified_by = relationship("User")


class OperationStatusLog(db.Model):
    __tablename__ = "operation_status_log"
    __table_args__ = (
        Index("ix_operation_status_log_case_changed", "operation_case_id", "changed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    operation_case_id: Mapped[int] = mapped_column(ForeignKey("operation_case.id"), nullable=False, index=True)
    from_status: Mapped[str] = mapped_column(db.String(40), nullable=False, default="")
    to_status: Mapped[str] = mapped_column(db.String(40), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False, index=True)
    changed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    reason: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    operation_case = relationship("OperationCase", back_populates="status_logs")
    changed_by = relationship("User")


class WorkOrderType(db.Model):
    __tablename__ = "work_order_type"
    __table_args__ = (
        UniqueConstraint("org_id", "code", name="uq_work_order_type_org_code"),
        Index("ix_work_order_type_org_active", "org_id", "active"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(db.String(40), nullable=False)
    name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    category: Mapped[WorkOrderCategory] = mapped_column(
        SAEnum(WorkOrderCategory, name="work_order_category"),
        nullable=False,
    )
    is_critical: Mapped[bool] = mapped_column(nullable=False, default=False)
    active: Mapped[bool] = mapped_column(nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class WorkOrder(db.Model):
    __tablename__ = "work_order"
    __table_args__ = (
        UniqueConstraint("org_id", "code", name="uq_work_order_org_code"),
        CheckConstraint(
            "(sepultura_id IS NOT NULL) OR (area_type IS NOT NULL AND (area_code IS NOT NULL OR location_text IS NOT NULL))",
            name="ck_work_order_location",
        ),
        Index("ix_work_order_org_status_due", "org_id", "status", "due_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(db.String(30), nullable=False)
    title: Mapped[str] = mapped_column(db.String(140), nullable=False)
    description: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    category: Mapped[WorkOrderCategory] = mapped_column(
        SAEnum(WorkOrderCategory, name="work_order_category"),
        nullable=False,
    )
    type_code: Mapped[str | None] = mapped_column(db.String(40), nullable=True, index=True)
    priority: Mapped[WorkOrderPriority] = mapped_column(
        SAEnum(WorkOrderPriority, name="work_order_priority"),
        nullable=False,
        default=WorkOrderPriority.MEDIA,
    )
    status: Mapped[WorkOrderStatus] = mapped_column(
        SAEnum(WorkOrderStatus, name="work_order_status"),
        nullable=False,
        default=WorkOrderStatus.BORRADOR,
    )
    operation_case_id: Mapped[int | None] = mapped_column(ForeignKey("operation_case.id"), nullable=True, index=True)
    sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True, index=True)
    area_type: Mapped[WorkOrderAreaType | None] = mapped_column(
        SAEnum(WorkOrderAreaType, name="work_order_area_type"),
        nullable=True,
    )
    area_code: Mapped[str | None] = mapped_column(db.String(60), nullable=True, default=None)
    location_text: Mapped[str | None] = mapped_column(db.String(255), nullable=True, default=None)
    assigned_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True, index=True)
    planned_start_at: Mapped[datetime | None] = mapped_column(nullable=True)
    planned_end_at: Mapped[datetime | None] = mapped_column(nullable=True)
    due_at: Mapped[datetime | None] = mapped_column(nullable=True, index=True)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(nullable=True)
    block_reason: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    cancel_reason: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    close_notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, nullable=False)

    sepultura = relationship("Sepultura")
    operation_case = relationship("OperationCase", back_populates="work_orders")
    assigned_user = relationship("User", foreign_keys=[assigned_user_id])
    created_by = relationship("User", foreign_keys=[created_by_user_id])
    updated_by = relationship("User", foreign_keys=[updated_by_user_id])


class WorkOrderTemplate(db.Model):
    __tablename__ = "work_order_template"
    __table_args__ = (
        UniqueConstraint("org_id", "code", name="uq_work_order_template_org_code"),
        Index("ix_work_order_template_org_active", "org_id", "active"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    code: Mapped[str] = mapped_column(db.String(40), nullable=False)
    name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    type_id: Mapped[int | None] = mapped_column(ForeignKey("work_order_type.id"), nullable=True, index=True)
    default_priority: Mapped[WorkOrderPriority] = mapped_column(
        SAEnum(WorkOrderPriority, name="work_order_priority"),
        nullable=False,
        default=WorkOrderPriority.MEDIA,
    )
    sla_hours: Mapped[int | None] = mapped_column(nullable=True)
    auto_create: Mapped[bool] = mapped_column(nullable=False, default=False)
    requires_sepultura: Mapped[bool] = mapped_column(nullable=False, default=False)
    allows_area: Mapped[bool] = mapped_column(nullable=False, default=True)
    active: Mapped[bool] = mapped_column(nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    type = relationship("WorkOrderType")


class WorkOrderTemplateChecklistItem(db.Model):
    __tablename__ = "work_order_template_checklist_item"
    __table_args__ = (
        Index("ix_wo_template_checklist_template", "template_id", "sort_order"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    template_id: Mapped[int] = mapped_column(ForeignKey("work_order_template.id"), nullable=False, index=True)
    label: Mapped[str] = mapped_column(db.String(255), nullable=False)
    required: Mapped[bool] = mapped_column(nullable=False, default=False)
    sort_order: Mapped[int] = mapped_column(nullable=False, default=0)

    template = relationship("WorkOrderTemplate")


class WorkOrderChecklistItem(db.Model):
    __tablename__ = "work_order_checklist_item"
    __table_args__ = (
        Index("ix_wo_checklist_work_order", "work_order_id", "sort_order"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    work_order_id: Mapped[int] = mapped_column(ForeignKey("work_order.id"), nullable=False, index=True)
    label: Mapped[str] = mapped_column(db.String(255), nullable=False)
    required: Mapped[bool] = mapped_column(nullable=False, default=False)
    done: Mapped[bool] = mapped_column(nullable=False, default=False)
    done_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    done_at: Mapped[datetime | None] = mapped_column(nullable=True)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")
    sort_order: Mapped[int] = mapped_column(nullable=False, default=0)

    work_order = relationship("WorkOrder")
    done_by = relationship("User")


class WorkOrderEvidence(db.Model):
    __tablename__ = "work_order_evidence"
    __table_args__ = (
        Index("ix_wo_evidence_work_order", "work_order_id", "uploaded_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    work_order_id: Mapped[int] = mapped_column(ForeignKey("work_order.id"), nullable=False, index=True)
    file_path: Mapped[str] = mapped_column(db.String(255), nullable=False)
    file_name: Mapped[str] = mapped_column(db.String(120), nullable=False)
    mime_type: Mapped[str] = mapped_column(db.String(120), nullable=False, default="application/octet-stream")
    uploaded_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    notes: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    work_order = relationship("WorkOrder")
    uploaded_by = relationship("User")


class WorkOrderDependency(db.Model):
    __tablename__ = "work_order_dependency"
    __table_args__ = (
        UniqueConstraint("work_order_id", "depends_on_work_order_id", name="uq_wo_dependency_pair"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    work_order_id: Mapped[int] = mapped_column(ForeignKey("work_order.id"), nullable=False, index=True)
    depends_on_work_order_id: Mapped[int] = mapped_column(ForeignKey("work_order.id"), nullable=False, index=True)
    dependency_type: Mapped[WorkOrderDependencyType] = mapped_column(
        SAEnum(WorkOrderDependencyType, name="work_order_dependency_type"),
        nullable=False,
        default=WorkOrderDependencyType.FINISH_TO_START,
    )

    work_order = relationship("WorkOrder", foreign_keys=[work_order_id])
    depends_on = relationship("WorkOrder", foreign_keys=[depends_on_work_order_id])


class WorkOrderEventRule(db.Model):
    __tablename__ = "work_order_event_rule"
    __table_args__ = (
        Index("ix_wo_event_rule_org_event_active", "org_id", "event_type", "active"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(db.String(60), nullable=False)
    template_id: Mapped[int] = mapped_column(ForeignKey("work_order_template.id"), nullable=False, index=True)
    conditions_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    active: Mapped[bool] = mapped_column(nullable=False, default=True)
    priority: Mapped[int] = mapped_column(nullable=False, default=100)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    template = relationship("WorkOrderTemplate")


class WorkOrderEventLog(db.Model):
    __tablename__ = "work_order_event_log"
    __table_args__ = (
        Index("ix_wo_event_log_org_event", "org_id", "event_type", "processed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(db.String(60), nullable=False)
    payload_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    processed_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    result: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")


class WorkOrderStatusLog(db.Model):
    __tablename__ = "work_order_status_log"
    __table_args__ = (
        Index("ix_wo_status_log_work_order_at", "work_order_id", "changed_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    work_order_id: Mapped[int] = mapped_column(ForeignKey("work_order.id"), nullable=False, index=True)
    from_status: Mapped[str] = mapped_column(db.String(40), nullable=False, default="")
    to_status: Mapped[str] = mapped_column(db.String(40), nullable=False)
    changed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    changed_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    reason: Mapped[str] = mapped_column(db.String(500), nullable=False, default="")

    work_order = relationship("WorkOrder")
    changed_by = relationship("User")


class LegacyExpediente(db.Model):
    __tablename__ = "legacy_expediente"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(nullable=False, index=True)
    numero: Mapped[str] = mapped_column(db.String(40), nullable=False)
    tipo: Mapped[str] = mapped_column(db.String(40), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    sepultura_id: Mapped[int | None] = mapped_column(nullable=True)
    difunto_id: Mapped[int | None] = mapped_column(nullable=True)
    declarante_id: Mapped[int | None] = mapped_column(nullable=True)
    fecha_prevista: Mapped[date | None] = mapped_column(nullable=True)
    notas: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class LegacyOrdenTrabajo(db.Model):
    __tablename__ = "legacy_orden_trabajo"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(nullable=False, index=True)
    expediente_id: Mapped[int | None] = mapped_column(nullable=True)
    titulo: Mapped[str] = mapped_column(db.String(120), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    notes: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


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


class BillingDocumentV2(db.Model):
    __tablename__ = "billing_document_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "number", name="uq_billing_document_v2_org_number"),
        CheckConstraint("total_amount >= 0", name="ck_billing_document_v2_total_non_negative"),
        CheckConstraint("residual_amount >= 0", name="ck_billing_document_v2_residual_non_negative"),
        Index("ix_billing_document_v2_org_status_issued_at", "org_id", "status", "issued_at"),
        Index("ix_billing_document_v2_org_contract_status", "org_id", "contract_id", "status"),
        Index("ix_billing_document_v2_org_fiscal_status", "org_id", "fiscal_status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    contract_id: Mapped[int | None] = mapped_column(ForeignKey("derecho_funerario_contrato.id"), nullable=True, index=True)
    sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True, index=True)
    original_document_id: Mapped[int | None] = mapped_column(ForeignKey("billing_document_v2.id"), nullable=True, index=True)
    document_type: Mapped[BillingDocumentType] = mapped_column(
        SAEnum(BillingDocumentType, name="billing_document_v2_type"),
        nullable=False,
    )
    status: Mapped[BillingDocumentStatus] = mapped_column(
        SAEnum(BillingDocumentStatus, name="billing_document_v2_status"),
        nullable=False,
        default=BillingDocumentStatus.DRAFT,
    )
    fiscal_status: Mapped[FiscalSubmissionStatus] = mapped_column(
        SAEnum(FiscalSubmissionStatus, name="billing_document_v2_fiscal_status"),
        nullable=False,
        default=FiscalSubmissionStatus.PENDING,
    )
    number: Mapped[str | None] = mapped_column(db.String(60), nullable=True)
    currency: Mapped[str] = mapped_column(db.String(3), nullable=False, default="EUR")
    total_amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    residual_amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    issued_at: Mapped[datetime | None] = mapped_column(nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, nullable=False)

    created_by = relationship("User")
    lines = relationship("BillingLineV2", back_populates="document", cascade="all, delete-orphan")
    payments = relationship("PaymentV2", back_populates="document")
    allocations = relationship("PaymentAllocationV2", back_populates="document")
    submissions = relationship("FiscalSubmissionV2", back_populates="document", cascade="all, delete-orphan")
    original_document = relationship("BillingDocumentV2", remote_side=[id], uselist=False)


class BillingLineV2(db.Model):
    __tablename__ = "billing_line_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "document_id", "line_no", name="uq_billing_line_v2_org_doc_line"),
        CheckConstraint("quantity > 0", name="ck_billing_line_v2_qty_positive"),
        CheckConstraint("unit_price >= 0", name="ck_billing_line_v2_price_non_negative"),
        CheckConstraint("total_amount >= 0", name="ck_billing_line_v2_total_non_negative"),
        Index("ix_billing_line_v2_org_document", "org_id", "document_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("billing_document_v2.id"), nullable=False, index=True)
    line_no: Mapped[int] = mapped_column(nullable=False)
    concept: Mapped[str] = mapped_column(db.String(255), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=1)
    unit_price: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    tax_rate: Mapped[Decimal] = mapped_column(db.Numeric(5, 2), nullable=False, default=0)
    net_amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    tax_amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    total_amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    document = relationship("BillingDocumentV2", back_populates="lines")


class PaymentV2(db.Model):
    __tablename__ = "payment_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "receipt_number", name="uq_payment_v2_org_receipt"),
        CheckConstraint("amount > 0", name="ck_payment_v2_amount_positive"),
        Index("ix_payment_v2_org_paid_at", "org_id", "paid_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("billing_document_v2.id"), nullable=False, index=True)
    amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False)
    method: Mapped[PaymentMethod] = mapped_column(
        SAEnum(PaymentMethod, name="payment_method_v2"),
        nullable=False,
        default=PaymentMethod.EFECTIVO,
    )
    receipt_number: Mapped[str] = mapped_column(db.String(60), nullable=False)
    external_reference: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    paid_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("user_account.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    document = relationship("BillingDocumentV2", back_populates="payments")
    created_by = relationship("User")
    allocations = relationship("PaymentAllocationV2", back_populates="payment", cascade="all, delete-orphan")


class PaymentAllocationV2(db.Model):
    __tablename__ = "payment_allocation_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "payment_id", "document_id", name="uq_payment_allocation_v2_org_payment_document"),
        CheckConstraint("amount > 0", name="ck_payment_allocation_v2_amount_positive"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payment_v2.id"), nullable=False, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("billing_document_v2.id"), nullable=False, index=True)
    amount: Mapped[Decimal] = mapped_column(db.Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    payment = relationship("PaymentV2", back_populates="allocations")
    document = relationship("BillingDocumentV2", back_populates="allocations")


class FiscalSubmissionV2(db.Model):
    __tablename__ = "fiscal_submission_v2"
    __table_args__ = (
        Index("ix_fiscal_submission_v2_org_status", "org_id", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("billing_document_v2.id"), nullable=False, index=True)
    status: Mapped[FiscalSubmissionStatus] = mapped_column(
        SAEnum(FiscalSubmissionStatus, name="fiscal_submission_v2_status"),
        nullable=False,
        default=FiscalSubmissionStatus.PENDING,
    )
    provider_name: Mapped[str] = mapped_column(db.String(80), nullable=False, default="")
    attempt_count: Mapped[int] = mapped_column(nullable=False, default=0)
    external_submission_id: Mapped[str] = mapped_column(db.String(120), nullable=False, default="")
    request_payload_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    response_payload_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    error_message: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    last_attempt_at: Mapped[datetime | None] = mapped_column(nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, nullable=False)

    document = relationship("BillingDocumentV2", back_populates="submissions")


class BillingSequenceV2(db.Model):
    __tablename__ = "billing_sequence_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "sequence_key", "year", name="uq_billing_sequence_v2_org_key_year"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    sequence_key: Mapped[str] = mapped_column(db.String(30), nullable=False)
    year: Mapped[int] = mapped_column(nullable=False)
    current_value: Mapped[int] = mapped_column(nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, nullable=False)


class IdempotencyRequestV2(db.Model):
    __tablename__ = "idempotency_request_v2"
    __table_args__ = (
        UniqueConstraint("org_id", "endpoint", "idempotency_key", name="uq_idempotency_request_v2_org_endpoint_key"),
        Index("ix_idempotency_request_v2_org_created", "org_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    endpoint: Mapped[str] = mapped_column(db.String(120), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(db.String(120), nullable=False)
    request_hash: Mapped[str] = mapped_column(db.String(64), nullable=False)
    response_status: Mapped[int] = mapped_column(nullable=False, default=0)
    response_json: Mapped[str] = mapped_column(db.Text(), nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


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
    declarante_id: Mapped[int | None] = mapped_column(ForeignKey("person.id"), nullable=True, index=True)
    fecha_prevista: Mapped[date | None] = mapped_column(nullable=True)
    notas: Mapped[str] = mapped_column(db.Text(), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)

    difunto = relationship("Person", foreign_keys=[difunto_id])
    declarante = relationship("Person", foreign_keys=[declarante_id])


class OrdenTrabajo(db.Model):
    # Spec 9.2 / 9.3 / 9.1.10 - preparado para MVP+
    __tablename__ = "orden_trabajo"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    expediente_id: Mapped[int | None] = mapped_column(ForeignKey("expediente.id"), nullable=True)
    titulo: Mapped[str] = mapped_column(db.String(120), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    notes: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(default=utcnow, nullable=False)


class LapidaStock(db.Model):
    # Spec 9.2.6 / 9.1.9 - preparado para MVP+
    __tablename__ = "lapida_stock"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    codigo: Mapped[str] = mapped_column(db.String(40), nullable=False)
    descripcion: Mapped[str] = mapped_column(db.String(120), nullable=False)
    estado: Mapped[str] = mapped_column(db.String(40), nullable=False)
    available_qty: Mapped[int] = mapped_column(nullable=False, default=0)
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


class LapidaStockMovimiento(db.Model):
    __tablename__ = "lapida_stock_movimiento"

    id: Mapped[int] = mapped_column(primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organization.id"), nullable=False, index=True)
    lapida_stock_id: Mapped[int] = mapped_column(ForeignKey("lapida_stock.id"), nullable=False, index=True)
    movimiento: Mapped[str] = mapped_column(db.String(20), nullable=False)
    quantity: Mapped[int] = mapped_column(nullable=False)
    sepultura_id: Mapped[int | None] = mapped_column(ForeignKey("sepultura.id"), nullable=True)
    notes: Mapped[str] = mapped_column(db.String(255), nullable=False, default="")
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
    connection.execute(
        ActivityLog.__table__.insert().values(
            org_id=target.org_id,
            sepultura_id=target.sepultura_id,
            action_type=MovimientoTipo.CONTRATO.value,
            details=f"Contrato {target.tipo.value} {target.fecha_inicio} - {target.fecha_fin}",
            created_at=utcnow(),
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
        connection.execute(
            ActivityLog.__table__.insert().values(
                org_id=target.org_id,
                sepultura_id=target.id,
                action_type=MovimientoTipo.CAMBIO_ESTADO.value,
                details=f"Cambio de estado a {target.estado.value}",
                created_at=utcnow(),
                user_id=None,
            )
        )


def seed_demo_data(session) -> None:
    def _assert_non_generic_person(*persons: Person) -> None:
        for person in persons:
            if is_generic_demo_name(person.first_name, person.last_name):
                raise ValueError(f"seed_demo_data generated invalid generic person name: {person.full_name}")

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
    comercial = User(
        email="comercial@smsft.local",
        full_name="Presentacion Comercial",
        password_hash=generate_password_hash("comercial123"),
    )
    operativo = User(
        email="operativo@smsft.local",
        full_name="Presentacion Operativa",
        password_hash=generate_password_hash("operativo123"),
    )
    session.add_all([admin, operario, comercial, operativo])
    session.flush()

    session.add_all(
        [
            Membership(user_id=admin.id, org_id=org.id, role="admin"),
            Membership(user_id=operario.id, org_id=org.id, role="operator"),
            Membership(user_id=comercial.id, org_id=org.id, role="operator"),
            Membership(user_id=operativo.id, org_id=org.id, role="admin"),
        ]
    )

    cemetery = Cemetery(
        org_id=org.id,
        name="Cementiri Municipal",
        location="Terrassa",
        municipality="Terrassa",
    )
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

    titular_1 = Person(
        org_id=org.id,
        first_name="Marta",
        last_name="Soler",
        dni_nif="11111111A",
        telefono="600111111",
        email="marta.soler@example.com",
        direccion="Carrer Major 10, Terrassa",
    )
    titular_2 = Person(
        org_id=org.id,
        first_name="Joan",
        last_name="Riera",
        dni_nif="22222222B",
        telefono="600222222",
        direccion="Carrer de la Font 4, Terrassa",
    )
    titular_3 = Person(
        org_id=org.id,
        first_name="Pere",
        last_name="Casals",
        dni_nif="44444444D",
        telefono="600444444",
        email="pere.casals@example.com",
    )
    difunto_1 = Person(
        org_id=org.id,
        first_name="Antoni",
        last_name="Ferrer",
        dni_nif="33333333C",
        notas="Registro historico de difunto para demo",
    )
    _assert_non_generic_person(titular_1, titular_2, titular_3, difunto_1)
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

    current_utc = utcnow()
    week_ago = current_utc - timedelta(days=7)
    two_weeks_ago = current_utc - timedelta(days=14)

    draft_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_1.id,
        sepultura_id=sep_1.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.DRAFT,
        fiscal_status=FiscalSubmissionStatus.PENDING,
        currency="EUR",
        total_amount=Decimal("45.00"),
        residual_amount=Decimal("45.00"),
        created_by_user_id=admin.id,
    )
    issued_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_2.id,
        sepultura_id=sep_5.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.ISSUED,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000001",
        currency="EUR",
        total_amount=Decimal("50.00"),
        residual_amount=Decimal("50.00"),
        issued_at=current_utc - timedelta(days=2),
        created_by_user_id=admin.id,
    )
    partial_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_1.id,
        sepultura_id=sep_1.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.PARTIALLY_PAID,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000002",
        currency="EUR",
        total_amount=Decimal("40.50"),
        residual_amount=Decimal("20.50"),
        issued_at=current_utc - timedelta(days=4),
        created_by_user_id=admin.id,
    )
    paid_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_3.id,
        sepultura_id=sep_2.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.PAID,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000003",
        currency="EUR",
        total_amount=Decimal("30.00"),
        residual_amount=Decimal("0.00"),
        issued_at=current_utc - timedelta(days=1),
        created_by_user_id=admin.id,
    )
    cancelled_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_2.id,
        sepultura_id=sep_5.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.CANCELLED,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000004",
        currency="EUR",
        total_amount=Decimal("35.00"),
        residual_amount=Decimal("0.00"),
        issued_at=week_ago,
        cancelled_at=week_ago + timedelta(hours=8),
        created_by_user_id=admin.id,
    )
    old_pending_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_legacy.id,
        sepultura_id=sep_6.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.ISSUED,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000005",
        currency="EUR",
        total_amount=Decimal("120.00"),
        residual_amount=Decimal("120.00"),
        issued_at=two_weeks_ago + timedelta(days=1),
        created_by_user_id=admin.id,
    )
    recent_paid_doc = BillingDocumentV2(
        org_id=org.id,
        contract_id=contrato_2.id,
        sepultura_id=sep_5.id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.PAID,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="F-DEMO-2026-000006",
        currency="EUR",
        total_amount=Decimal("85.00"),
        residual_amount=Decimal("0.00"),
        issued_at=current_utc - timedelta(days=3),
        created_by_user_id=admin.id,
    )
    session.add_all([draft_doc, issued_doc, partial_doc, paid_doc, cancelled_doc, old_pending_doc, recent_paid_doc])
    session.flush()

    credit_note = BillingDocumentV2(
        org_id=org.id,
        contract_id=cancelled_doc.contract_id,
        sepultura_id=cancelled_doc.sepultura_id,
        original_document_id=cancelled_doc.id,
        document_type=BillingDocumentType.CREDIT_NOTE,
        status=BillingDocumentStatus.ISSUED,
        fiscal_status=FiscalSubmissionStatus.ACCEPTED,
        number="NC-DEMO-2026-000001",
        currency="EUR",
        total_amount=Decimal("35.00"),
        residual_amount=Decimal("0.00"),
        issued_at=week_ago + timedelta(days=1),
        created_by_user_id=admin.id,
    )
    session.add(credit_note)
    session.flush()

    session.add_all(
        [
            BillingLineV2(
                org_id=org.id,
                document_id=draft_doc.id,
                line_no=1,
                concept="Mantenimiento anual",
                quantity=Decimal("1.00"),
                unit_price=Decimal("45.00"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("45.00"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("45.00"),
            ),
            BillingLineV2(
                org_id=org.id,
                document_id=issued_doc.id,
                line_no=1,
                concept="Regularizacion contrato",
                quantity=Decimal("1.00"),
                unit_price=Decimal("50.00"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("50.00"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("50.00"),
            ),
            BillingLineV2(
                org_id=org.id,
                document_id=partial_doc.id,
                line_no=1,
                concept="Mantenimiento pensionista",
                quantity=Decimal("1.00"),
                unit_price=Decimal("40.50"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("40.50"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("40.50"),
            ),
            BillingLineV2(
                org_id=org.id,
                document_id=paid_doc.id,
                line_no=1,
                concept="Mantenimiento uso inmediato",
                quantity=Decimal("1.00"),
                unit_price=Decimal("30.00"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("30.00"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("30.00"),
            ),
            BillingLineV2(
                org_id=org.id,
                document_id=cancelled_doc.id,
                line_no=1,
                concept="Factura anulada por rectificacion",
                quantity=Decimal("1.00"),
                unit_price=Decimal("35.00"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("35.00"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("35.00"),
            ),
            BillingLineV2(
                org_id=org.id,
                document_id=credit_note.id,
                line_no=1,
                concept="Rectificacion total",
                quantity=Decimal("1.00"),
                unit_price=Decimal("35.00"),
                tax_rate=Decimal("0.00"),
                net_amount=Decimal("35.00"),
                tax_amount=Decimal("0.00"),
                total_amount=Decimal("35.00"),
            ),
        ]
    )

    partial_payment = PaymentV2(
        org_id=org.id,
        document_id=partial_doc.id,
        amount=Decimal("20.00"),
        method=PaymentMethod.TARJETA,
        receipt_number="R-DEMO-2026-000001",
        external_reference="seed-partial",
        created_by_user_id=admin.id,
        paid_at=current_utc - timedelta(days=2),
    )
    paid_payment = PaymentV2(
        org_id=org.id,
        document_id=paid_doc.id,
        amount=Decimal("30.00"),
        method=PaymentMethod.EFECTIVO,
        receipt_number="R-DEMO-2026-000002",
        external_reference="seed-paid",
        created_by_user_id=admin.id,
        paid_at=week_ago + timedelta(days=2),
    )
    recent_payment = PaymentV2(
        org_id=org.id,
        document_id=recent_paid_doc.id,
        amount=Decimal("85.00"),
        method=PaymentMethod.TRANSFERENCIA,
        receipt_number="R-DEMO-2026-000003",
        external_reference="seed-recent-paid",
        created_by_user_id=admin.id,
        paid_at=current_utc - timedelta(days=1),
    )
    session.add_all([partial_payment, paid_payment, recent_payment])
    session.flush()
    session.add_all(
        [
            PaymentAllocationV2(
                org_id=org.id,
                payment_id=partial_payment.id,
                document_id=partial_doc.id,
                amount=Decimal("20.00"),
            ),
            PaymentAllocationV2(
                org_id=org.id,
                payment_id=paid_payment.id,
                document_id=paid_doc.id,
                amount=Decimal("30.00"),
            ),
            PaymentAllocationV2(
                org_id=org.id,
                payment_id=recent_payment.id,
                document_id=recent_paid_doc.id,
                amount=Decimal("85.00"),
            ),
        ]
    )

    session.add_all(
        [
            FiscalSubmissionV2(
                org_id=org.id,
                document_id=issued_doc.id,
                status=FiscalSubmissionStatus.ACCEPTED,
                provider_name="demo_provider",
                attempt_count=1,
                external_submission_id="SUB-DEMO-0001",
                request_payload_json='{"mode":"seed"}',
                response_payload_json='{"status":"ok"}',
                accepted_at=utcnow(),
            ),
            FiscalSubmissionV2(
                org_id=org.id,
                document_id=partial_doc.id,
                status=FiscalSubmissionStatus.ACCEPTED,
                provider_name="demo_provider",
                attempt_count=1,
                external_submission_id="SUB-DEMO-0002",
                request_payload_json='{"mode":"seed"}',
                response_payload_json='{"status":"ok"}',
                accepted_at=utcnow(),
            ),
            FiscalSubmissionV2(
                org_id=org.id,
                document_id=cancelled_doc.id,
                status=FiscalSubmissionStatus.RETRYING,
                provider_name="blocked_no_provider",
                attempt_count=2,
                request_payload_json='{"mode":"seed"}',
                response_payload_json='{"status":"blocked"}',
                error_message="Integracion fiscal bloqueada: proveedor no definido para envio VeriFactu",
            ),
            FiscalSubmissionV2(
                org_id=org.id,
                document_id=credit_note.id,
                status=FiscalSubmissionStatus.ACCEPTED,
                provider_name="demo_provider",
                attempt_count=1,
                external_submission_id="SUB-DEMO-0003",
                request_payload_json='{"mode":"seed"}',
                response_payload_json='{"status":"ok"}',
                accepted_at=utcnow(),
            ),
        ]
    )

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
        ]
    )

    session.add_all(
        [
            Expediente(
                org_id=org.id,
                numero="C-2026-0012",
                tipo="INHUMACION",
                estado="EN_TRAMITE",
                sepultura_id=sep_1.id,
                difunto_id=difunto_1.id,
                fecha_prevista=date(2026, 3, 3),
                notas="Expediente de inhumacion demo",
            ),
            Expediente(
                org_id=org.id,
                numero="C-2026-0011",
                tipo="EXHUMACION",
                estado="ABIERTO",
                sepultura_id=sep_5.id,
                fecha_prevista=date(2026, 3, 10),
            ),
            Expediente(
                org_id=org.id,
                numero="C-2026-0010",
                tipo="INHUMACION",
                estado="FINALIZADO",
                sepultura_id=sep_1.id,
                fecha_prevista=date(2026, 2, 20),
                notas="Expediente finalizado para demo de historico",
            ),
            OrdenTrabajo(org_id=org.id, titulo="Preparar lapida", estado="PENDIENTE"),
            OrdenTrabajo(org_id=org.id, titulo="Revision bloque B-12", estado="PENDIENTE"),
            OrdenTrabajo(org_id=org.id, titulo="Limpieza pasillo V-3", estado="EN_CURSO"),
            LapidaStock(
                org_id=org.id,
                codigo="LAP-STD",
                descripcion="Lapida estandar resina",
                estado="ACTIVO",
                available_qty=8,
            ),
            LapidaStock(
                org_id=org.id,
                codigo="LAP-MRB",
                descripcion="Lapida marmol premium",
                estado="ACTIVO",
                available_qty=3,
            ),
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

    wo_type_inhum = WorkOrderType(
        org_id=org.id,
        code="INHUMACION",
        name="Inhumacion operativa",
        category=WorkOrderCategory.FUNERARIA,
        is_critical=True,
        active=True,
    )
    wo_type_exhum = WorkOrderType(
        org_id=org.id,
        code="EXHUMACION",
        name="Exhumacion operativa",
        category=WorkOrderCategory.FUNERARIA,
        is_critical=True,
        active=True,
    )
    wo_type_docs = WorkOrderType(
        org_id=org.id,
        code="ACTUALIZACION_DOC",
        name="Actualizacion documental",
        category=WorkOrderCategory.ADMINISTRATIVA,
        is_critical=False,
        active=True,
    )
    wo_type_lap = WorkOrderType(
        org_id=org.id,
        code="LAPIDA_COORD",
        name="Coordinacion lapida",
        category=WorkOrderCategory.MANTENIMIENTO,
        is_critical=False,
        active=True,
    )
    wo_type_stock = WorkOrderType(
        org_id=org.id,
        code="APROVISIONAMIENTO",
        name="Aprovisionamiento",
        category=WorkOrderCategory.MANTENIMIENTO,
        is_critical=False,
        active=True,
    )
    session.add_all([wo_type_inhum, wo_type_exhum, wo_type_docs, wo_type_lap, wo_type_stock])
    session.flush()

    tpl_inhum = WorkOrderTemplate(
        org_id=org.id,
        code="TPL_INHUMACION_BASE",
        name="Bundle inhumacion base",
        type_id=wo_type_inhum.id,
        default_priority=WorkOrderPriority.ALTA,
        sla_hours=24,
        auto_create=True,
        requires_sepultura=True,
        allows_area=False,
        active=True,
    )
    tpl_exhum = WorkOrderTemplate(
        org_id=org.id,
        code="TPL_EXHUMACION_BASE",
        name="Bundle exhumacion base",
        type_id=wo_type_exhum.id,
        default_priority=WorkOrderPriority.ALTA,
        sla_hours=24,
        auto_create=True,
        requires_sepultura=True,
        allows_area=False,
        active=True,
    )
    tpl_docs = WorkOrderTemplate(
        org_id=org.id,
        code="TPL_DOCS_OWNERSHIP",
        name="Actualizacion documental titularidad",
        type_id=wo_type_docs.id,
        default_priority=WorkOrderPriority.MEDIA,
        sla_hours=72,
        auto_create=True,
        requires_sepultura=False,
        allows_area=True,
        active=True,
    )
    tpl_lap = WorkOrderTemplate(
        org_id=org.id,
        code="TPL_LAPIDA_COORD",
        name="Seguimiento lapida",
        type_id=wo_type_lap.id,
        default_priority=WorkOrderPriority.MEDIA,
        sla_hours=48,
        auto_create=True,
        requires_sepultura=True,
        allows_area=False,
        active=True,
    )
    tpl_stock = WorkOrderTemplate(
        org_id=org.id,
        code="TPL_STOCK_BAJO",
        name="Stock bajo lapidas",
        type_id=wo_type_stock.id,
        default_priority=WorkOrderPriority.ALTA,
        sla_hours=48,
        auto_create=True,
        requires_sepultura=False,
        allows_area=True,
        active=True,
    )
    session.add_all([tpl_inhum, tpl_exhum, tpl_docs, tpl_lap, tpl_stock])
    session.flush()

    session.add_all(
        [
            WorkOrderTemplateChecklistItem(
                template_id=tpl_inhum.id,
                label="Verificacion documental final",
                required=True,
                sort_order=1,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_inhum.id,
                label="Preparacion de unidad",
                required=True,
                sort_order=2,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_inhum.id,
                label="Cierre y confirmacion",
                required=True,
                sort_order=3,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_exhum.id,
                label="Validacion legal exhumacion",
                required=True,
                sort_order=1,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_exhum.id,
                label="Ejecucion exhumacion",
                required=True,
                sort_order=2,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_docs.id,
                label="Actualizar expediente documental",
                required=True,
                sort_order=1,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_lap.id,
                label="Confirmar texto y estado de placa",
                required=False,
                sort_order=1,
            ),
            WorkOrderTemplateChecklistItem(
                template_id=tpl_stock.id,
                label="Solicitar reposicion a proveedor",
                required=True,
                sort_order=1,
            ),
        ]
    )

    session.add_all(
        [
            WorkOrderEventRule(
                org_id=org.id,
                event_type="DECEASED_ADDED_TO_SEPULTURA",
                template_id=tpl_inhum.id,
                conditions_json="{}",
                active=True,
                priority=10,
            ),
            WorkOrderEventRule(
                org_id=org.id,
                event_type="DECEASED_REMOVED_FROM_SEPULTURA",
                template_id=tpl_exhum.id,
                conditions_json="{}",
                active=True,
                priority=10,
            ),
            WorkOrderEventRule(
                org_id=org.id,
                event_type="OWNERSHIP_CASE_APPROVED",
                template_id=tpl_docs.id,
                conditions_json="{}",
                active=True,
                priority=10,
            ),
            WorkOrderEventRule(
                org_id=org.id,
                event_type="LAPIDA_ORDER_CREATED",
                template_id=tpl_lap.id,
                conditions_json="{}",
                active=True,
                priority=10,
            ),
            WorkOrderEventRule(
                org_id=org.id,
                event_type="LOW_STOCK_DETECTED",
                template_id=tpl_stock.id,
                conditions_json="{}",
                active=True,
                priority=10,
            ),
        ]
    )

    session.add_all(
        [
            WorkOrder(
                org_id=org.id,
                code="OT-2026-000001",
                title="Revision inicial de zona B-12",
                description="OT de ejemplo para panel operativo",
                category=WorkOrderCategory.MANTENIMIENTO,
                type_code=wo_type_lap.code,
                priority=WorkOrderPriority.MEDIA,
                status=WorkOrderStatus.PLANIFICADA,
                sepultura_id=sep_1.id,
                assigned_user_id=operario.id,
                planned_start_at=current_utc + timedelta(hours=3),
                due_at=current_utc + timedelta(hours=26),
                created_at=current_utc - timedelta(days=1),
                created_by_user_id=admin.id,
                updated_by_user_id=admin.id,
            ),
            WorkOrder(
                org_id=org.id,
                code="OT-2026-000002",
                title="Validar expediente exhumacion B-20",
                description="Revisión documental pendiente de firma",
                category=WorkOrderCategory.ADMINISTRATIVA,
                type_code=wo_type_docs.code,
                priority=WorkOrderPriority.ALTA,
                status=WorkOrderStatus.EN_CURSO,
                sepultura_id=sep_5.id,
                assigned_user_id=admin.id,
                started_at=current_utc - timedelta(hours=5),
                due_at=current_utc - timedelta(hours=2),
                created_at=current_utc - timedelta(days=2),
                created_by_user_id=admin.id,
                updated_by_user_id=admin.id,
            ),
            WorkOrder(
                org_id=org.id,
                code="OT-2026-000003",
                title="Preparación inhumación B-12",
                description="Checklist completo y cierre operativo",
                category=WorkOrderCategory.FUNERARIA,
                type_code=wo_type_inhum.code,
                priority=WorkOrderPriority.ALTA,
                status=WorkOrderStatus.COMPLETADA,
                sepultura_id=sep_1.id,
                assigned_user_id=operario.id,
                started_at=current_utc - timedelta(days=3, hours=6),
                completed_at=current_utc - timedelta(days=3, hours=1),
                due_at=current_utc - timedelta(days=3) + timedelta(hours=8),
                created_at=current_utc - timedelta(days=4),
                created_by_user_id=admin.id,
                updated_by_user_id=admin.id,
            ),
            WorkOrder(
                org_id=org.id,
                code="OT-2026-000004",
                title="Exhumación programada B-22",
                description="Reprogramada por disponibilidad de equipo",
                category=WorkOrderCategory.FUNERARIA,
                type_code=wo_type_exhum.code,
                priority=WorkOrderPriority.URGENTE,
                status=WorkOrderStatus.ASIGNADA,
                sepultura_id=sep_6.id,
                assigned_user_id=operario.id,
                due_at=current_utc + timedelta(hours=10),
                created_at=current_utc - timedelta(hours=20),
                created_by_user_id=admin.id,
                updated_by_user_id=admin.id,
            ),
            WorkOrder(
                org_id=org.id,
                code="OT-2026-000005",
                title="Stock lapidas bajo mínimo",
                description="Lanzar pedido y confirmar fecha de entrega",
                category=WorkOrderCategory.MANTENIMIENTO,
                type_code=wo_type_stock.code,
                priority=WorkOrderPriority.MEDIA,
                status=WorkOrderStatus.COMPLETADA,
                area_type=WorkOrderAreaType.GENERAL,
                area_code="ALM-CEN-01",
                location_text="Almacén central",
                assigned_user_id=admin.id,
                started_at=week_ago + timedelta(days=1),
                completed_at=week_ago + timedelta(days=1, hours=6),
                due_at=week_ago + timedelta(days=2),
                created_at=week_ago,
                created_by_user_id=admin.id,
                updated_by_user_id=admin.id,
            ),
        ]
    )

    successor_1 = Person(
        org_id=org.id,
        first_name="Carla",
        last_name="Mora",
        dni_nif="88888888H",
        email="carla.mora@example.com",
    )
    successor_2 = Person(
        org_id=org.id,
        first_name="Sonia",
        last_name="Pons",
        dni_nif="99999999J",
    )
    successor_3 = Person(
        org_id=org.id,
        first_name="Marc",
        last_name="Vila",
        dni_nif="10101010K",
        telefono="600101010",
    )
    extra_person_1 = Person(
        org_id=org.id,
        first_name="Lucia",
        last_name="Navarro",
        dni_nif="12121212L",
        telefono="600121212",
        email="lucia.navarro@example.com",
    )
    extra_person_2 = Person(
        org_id=org.id,
        first_name="Ramon",
        last_name="Ibanez",
        telefono="600131313",
        direccion="Avinguda Barcelona 22, Terrassa",
    )
    extra_person_3 = Person(
        org_id=org.id,
        first_name="Elisabet",
        last_name="Puig",
        dni_nif="14141414M",
        notas="Contacto habitual para tramites",
    )
    _assert_non_generic_person(
        successor_1,
        successor_2,
        successor_3,
        extra_person_1,
        extra_person_2,
        extra_person_3,
    )
    session.add_all(
        [
            successor_1,
            successor_2,
            successor_3,
            extra_person_1,
            extra_person_2,
            extra_person_3,
        ]
    )
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
        checklist = OWNERSHIP_CASE_CHECKLIST[case.type]
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
