from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.core.config import Config
from app.core.extensions import db
from app.core.models import (
    Cemetery,
    Membership,
    Organization,
    Sepultura,
    SepulturaEstado,
    User,
    seed_demo_data,
)


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite://"
    SECRET_KEY = "test-secret"


@pytest.fixture
def app():
    app = create_app(TestConfig)
    with app.app_context():
        db.create_all()
        seed_demo_data(db.session)
        yield app
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def login_admin(client):
    def _login():
        return client.post(
            "/auth/login",
            data={"email": "admin@smsft.local", "password": "admin123"},
            follow_redirects=True,
        )

    return _login


@pytest.fixture
def login_operator(client):
    def _login():
        return client.post(
            "/auth/login",
            data={"email": "operario@smsft.local", "password": "operario123"},
            follow_redirects=True,
        )

    return _login


@pytest.fixture
def second_org_sepultura(app):
    with app.app_context():
        org2 = Organization(name="Org Two", code="ORG2")
        user2 = User(email="org2@example.com", full_name="User Org2", password_hash="x")
        db.session.add_all([org2, user2])
        db.session.flush()
        db.session.add(Membership(user_id=user2.id, org_id=org2.id, role="admin"))
        cemetery = Cemetery(org_id=org2.id, name="Cementeri 2", location="X")
        db.session.add(cemetery)
        db.session.flush()
        sep = Sepultura(
            org_id=org2.id,
            cemetery_id=cemetery.id,
            bloque="ORG2",
            fila=1,
            columna=1,
            via="V-1",
            numero=1,
            modalidad="Nínxol",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Nínxols",
            tipo_lapida="Resina",
            orientacion="Nord",
        )
        db.session.add(sep)
        db.session.commit()
        return sep.id
