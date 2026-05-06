# backend/config/

Módulo de configuración de la conexión a las bases de datos PostgreSQL/SQL Server mediante SQLAlchemy.

## Archivo: `database.py`

Provee todos los objetos necesarios para que el resto del backend interactúe con PostgreSQL/SQL Server.

### Objetos exportados

| Objeto | Tipo | Descripción |
|---|---|---|
| `Base` | `DeclarativeBase` | Clase base compartida por todos los modelos ORM. Todos los modelos deben heredar de este `Base` para que `create_all` los registre. |
| `engine` | `Engine` | Motor SQLAlchemy conectado a PostgreSQL/SQL Server. |
| `SessionLocal` | `sessionmaker` | Fábrica de sesiones de base de datos. |
| `metadata` | `MetaData` | Objeto de metadata de SQLAlchemy. |
| `get_db` | `generator` | Dependencia FastAPI que provee una sesión de DB y la cierra al finalizar el request. |
| `DatabaseException` | `Exception` | Excepción personalizada para errores de base de datos. |
| `handle_db_exceptions` | `decorator` | Decorador que captura `SQLAlchemyError` y lo convierte en `DatabaseException`. |

### Resolución de la URL de conexión

La URL se construye con la siguiente prioridad:

1. Variable de entorno `DATABASE_URL` (inyectada por docker-compose en producción).
2. Variables individuales `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` (utilizadas en desarrollo local con `.env`).
3. Variables individuales `SQL_SERVER_USER`, `SQL_SERVER_PASSWORD`, `SQL_SERVER_HOST`, `SQL_SERVER_PORT`, `SQL_SERVER_DB` (utilizadas en desarrollo local con `.env`).

### Uso en los routers (FastAPI Dependency Injection)

```python
from config.database import get_db
from sqlalchemy.orm import Session
from fastapi import Depends

@router.post("/etl")
async def run_etl(db: Session = Depends(get_db)):
    # db es una sesión activa, se cierra automáticamente al finalizar
    ...
```

### Uso en los modelos

```python
from config.database import Base
from sqlalchemy import Column, Integer, String

class MiModelo(Base):
    __tablename__ = "mi_tabla"
    __table_args__ = {"schema": "public"}
    id = Column(Integer, primary_key=True)
    nombre = Column(String(255))
```
