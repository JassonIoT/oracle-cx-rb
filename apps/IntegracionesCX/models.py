from pydantic import BaseModel


class ServiceClient(BaseModel):
    PartyNumber: str
    Nombre: str
    Sectores: str
    Pais: str
    Departamento_Estado: str
    Ciudad: str
    Direccion1: str
    Direccion2: str
    TipoOrg: str
    Responsable: str
    Contacto_pri: str
    Telefono_pri: int
    Correo_pri: str
    URL: str

class ServiceContact(BaseModel):
    PartyNumber: str
    PrimerNombre: str
    Apellidos: str
    Cargo: str
    TelefonoMovil: str
    TelefonoTrabajo: str
    Correo: str
    Responsable: str
    EstadoContacto: str


class Containers(BaseModel):
    carrier_id: str
    container_id: str


class Container(BaseModel):
    container_id: str


class NameDatabase(BaseModel):
    name_db: str