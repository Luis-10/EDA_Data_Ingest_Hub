"""
Servicio de API de Auditsa
Maneja las conexiones y obtención de datos de la API
"""

import requests
import json
import logging
import os
import dotenv
from typing import List, Dict, Optional
from datetime import datetime

# Cargar variables de entorno
dotenv.load_dotenv()

# Configurar logging
logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)


class AuditsaApiService:
    """Servicio para interactuar con la API de Auditsa"""

    def __init__(self):
        self.base_url = os.getenv("AUDITSA_URL")
        self.token = os.getenv("AUDITSA_TOKEN")
        self.client_id = os.getenv("AUDITSA_CLID")
        self.logger = logger

    def get_tv_data(self, fecha: str) -> List[Dict]:
        """
        Obtiene datos de TV (dedicado) para una fecha específica
        
        Args: 
            fecha: Fecha en formato YYYYMMDD
        
        Returns: Lista de diccionarios con datos de TV
        """
        try:
            fecha_inicio = f"{fecha}{os.getenv('STR_START')}"
            fecha_fin = f"{fecha}{os.getenv('STR_END')}"

            api_url = (
                f"{self.base_url}/GetHits"
                f"?tkn={self.token}"
                f"&clid={self.client_id}"
                f"&perIni={fecha_inicio}"
                f"&perFin={fecha_fin}"
            )

            self.logger.info(f"Consultando API TV para fecha: {fecha}")

            response = requests.get(api_url, timeout=300)  # 5 minutos timeout
            response.raise_for_status()

            self.logger.info(f"Respuesta API TV - Status: {response.status_code}")

            data = json.loads(response.content)
            self.logger.info(f"Datos obtenidos: {len(data)} registros de TV")

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error en petición API TV: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando respuesta JSON TV: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error general obteniendo datos TV: {str(e)}")
            raise

    def get_radio_data(self, fecha: str, medids: int) -> List[Dict]:
        """
        Obtiene datos de Radio (automático) para una fecha específica
        
        Args: 
            fecha: Fecha en formato YYYYMMDD
        
        Returns: Lista de diccionarios con datos de Radio
        """
        try:
            fecha_inicio = f"{fecha}{os.getenv('STR_START')}"
            fecha_fin = f"{fecha}{os.getenv('STR_END')}"

            api_url = (
                f"{self.base_url}/GetHitsAutomatico"
                f"?tkn={self.token}"
                f"&clid={self.client_id}"
                f"&perIni={fecha_inicio}"
                f"&perFin={fecha_fin}"
                f"&medids={medids}"
            )

            self.logger.info(f"Consultando API Radio para fecha: {fecha}")

            response = requests.get(api_url, timeout=300)
            response.raise_for_status()

            self.logger.info(f"Respuesta API Radio - Status: {response.status_code}")

            data = json.loads(response.content)
            self.logger.info(f"Datos obtenidos: {len(data)} registros de Radio")

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error en petición API Radio: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando respuesta JSON Radio: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error general obteniendo datos Radio: {str(e)}")
            raise

    def get_impresos_data(self, fecha: str, medids: Optional[int] = None) -> List[Dict]:
        """
        Obtiene datos de Impresos para una fecha específica
        
        Args: 
            fecha: Fecha en formato YYYYMMDD
            medids: ID del medio de Impresos (1, 2, 4) - Opcional
        
        Returns: Lista de diccionarios con datos de Impresos
        """
        try:
            fecha_inicio = f"{fecha}"
            fecha_fin = f"{fecha}"

            api_url = (
                f"{self.base_url}/GetPrintedHits"
                f"?tkn={self.token}"
                f"&clid={self.client_id}"
                f"&perIni={fecha_inicio}"
                f"&perFin={fecha_fin}"
            )
            
            # Agregar medids si se especifica
            if medids is not None:
                api_url += f"&medids={medids}"

            self.logger.info(f"Consultando API Impresos para fecha: {fecha}")

            response = requests.get(api_url, timeout=300)
            response.raise_for_status()

            self.logger.info(f"Respuesta API Impresos - Status: {response.status_code}")

            data = json.loads(response.content)
            self.logger.info(f"Datos obtenidos: {len(data)} registros de Impresos")

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error en petición API Impresos: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando respuesta JSON Impresos: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error general obteniendo datos Impresos: {str(e)}")
            raise

    def get_media_master_library_data(self, fecha_inicio: str, fecha_fin: str) -> List[Dict]:
        """
        Obtiene datos del maestro de medios para un rango de fechas

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD

        Returns: Lista de diccionarios con datos del maestro de medios
        """
        try:
            api_url = (
                f"{self.base_url}/GetMediaMasterLibrary"
                f"?tkn={self.token}"
                f"&clid={self.client_id}"
                f"&perIni={fecha_inicio}"
                f"&perFin={fecha_fin}"
            )

            self.logger.info(f"Consultando API MediaMasterLibrary: {fecha_inicio} - {fecha_fin}")

            response = requests.get(api_url, timeout=300)
            response.raise_for_status()

            self.logger.info(f"Respuesta API MediaMasterLibrary - Status: {response.status_code}")

            data = json.loads(response.content)
            self.logger.info(f"Datos obtenidos: {len(data)} registros de MediaMasterLibrary")

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error en petición API MediaMasterLibrary: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decodificando respuesta JSON MediaMasterLibrary: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error general obteniendo datos MediaMasterLibrary: {str(e)}")
            raise

    def get_media_master_totals_data(
        self,
        fecha_inicio: str,
        fecha_fin: str,
        extra_param: Optional[str] = None,
    ) -> List[Dict]:
        """
        Obtiene totales agregados del maestro de medios para un rango de fechas.

        Args:
            fecha_inicio: Fecha de inicio en formato YYYYMMDD
            fecha_fin: Fecha de fin en formato YYYYMMDD
            extra_param: Nombre del parámetro booleano adicional opcional (valor siempre true).
                         Ej: extra_param="byMedio" → &byMedio=true

        Returns: Lista de diccionarios con totales del maestro de medios
        """
        try:
            api_url = (
                f"{self.base_url}/GetMediaMasterTotals"
                f"?tkn={self.token}"
                f"&clid={self.client_id}"
                f"&perIni={fecha_inicio}"
                f"&perFin={fecha_fin}"
            )

            if extra_param:
                api_url += f"&{extra_param}=true"

            self.logger.info(
                f"Consultando API MediaMasterTotals: {fecha_inicio} - {fecha_fin}"
            )

            response = requests.get(api_url, timeout=300)
            response.raise_for_status()

            self.logger.info(
                f"Respuesta API MediaMasterTotals - Status: {response.status_code}"
            )

            data = json.loads(response.content)
            self.logger.info(
                f"Datos obtenidos: {len(data)} registros de MediaMasterTotals"
            )

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error en petición API MediaMasterTotals: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Error decodificando respuesta JSON MediaMasterTotals: {str(e)}"
            )
            raise
        except Exception as e:
            self.logger.error(
                f"Error general obteniendo datos MediaMasterTotals: {str(e)}"
            )
            raise

    def test_connection(self) -> bool:
        """
        Prueba la conexión con la API
        
        Returns: True si la conexión es exitosa, False en caso contrario
        """
        try:
            # Probar con una fecha reciente
            yesterday = datetime.now().strftime("%Y%m%d")
            data = self.get_tv_data(yesterday)

            self.logger.info("Conexión con API exitosa")
            return True

        except Exception as e:
            self.logger.error(f"Error probando conexión API: {str(e)}")
            return False
