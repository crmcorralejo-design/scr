"""
BookingScraper/app/vpn_manager_windows.py
Gestor NordVPN para Windows - BookingScraper Pro
Windows 11 + Python 3.14.3

CORRECCIONES v1.1:
  [FIX] _connect_manual: input() BLOQUEANTE eliminado
  [FIX] shell=True con lista de args reemplazado por shell=False + string unico
  [FIX] __del__: captura AttributeError si __init__ fallo a medias
  [NEW] Parametro interactive=False para modo Celery (sin prompts de usuario)
  [NEW] connect_or_raise(): para uso en tareas automatizadas

CORRECCIONES v2.3:
  [FIX CRITICO] verify_vpn_active(): cuando original_ip='Unknown' O current='Unknown'
               asumir VPN activa en lugar de inactiva.
               CAUSA RAIZ de ERR_NAME_NOT_RESOLVED: 10 threads simultaneos agotaban
               los servicios externos de IP (rate-limit), todos devolvian 'Unknown',
               verify_vpn_active() retornaba False, y los 10 threads intentaban
               reconectar a 10 paises distintos simultaneamente -> DNS inestable ->
               Brave no podia resolver booking.com -> ERR_NAME_NOT_RESOLVED en todo.
  [FIX CRITICO] get_current_ip(): anadida cache de 30s con threading.Lock.
               Evita que multiples threads simultaneos saturen los servicios externos.
"""

import subprocess
import time
import random
import platform
import os
import threading
from typing import Optional, Dict

import requests
from loguru import logger

# winreg solo disponible en Windows
try:
    import winreg
    _WINREG_AVAILABLE = True
except ImportError:
    _WINREG_AVAILABLE = False


class NordVPNManagerWindows:
    """Gestor de NordVPN para Windows 11"""

    # Países disponibles con sus nombres completos
    COUNTRY_NAMES: Dict[str, str] = {
        "US": "United States",
        "UK": "United Kingdom",
        "DE": "Germany",
        "FR": "France",
        "NL": "Netherlands",
        "ES": "Spain",
        "IT": "Italy",
        "CA": "Canada",
        "SE": "Sweden",
        "CH": "Switzerland",
    }

    def __init__(
        self,
        method: str = "auto",
        interactive: bool = False,          # ✅ NEW: False = modo Celery (sin input())
        max_connections_per_server: int = 50,
    ):
        """
        Args:
            method:       'auto' | 'cli' | 'app' | 'manual'
            interactive:  True = permite prompts de usuario (modo consola)
                          False = lanza excepción si se necesita acción manual
            max_connections_per_server: rotación automática al alcanzar este límite
        """
        self.interactive = interactive
        self.max_connections_per_server = max_connections_per_server
        self.current_server: Optional[str] = None
        self.current_ip:     Optional[str] = None
        self.original_ip:    Optional[str] = None
        self.connection_count = 0

        # Cache de IP para evitar saturar servicios externos con multiples threads
        self._ip_cache_value: str   = "Unknown"
        self._ip_cache_time:  float = 0.0
        self._ip_cache_ttl:   float = 30.0
        self._ip_cache_lock   = threading.Lock()

        self.method = self._detect_method() if method == "auto" else method

        self._detect_original_ip()

        logger.info(
            f"VPN Manager Windows inicializado | método={self.method} "
            f"| interactive={interactive} | sistema={platform.version()}"
        )

    # ── DETECCIÓN AUTOMÁTICA ───────────────────────────────────────────────────

    def _detect_method(self) -> str:
        """Detecta el método VPN disponible en el sistema."""

        # ✅ FIX: shell=False con string único (no lista con shell=True)
        try:
            # [FIX v2.4] shell=False + lista de args. En Windows, nordvpn.exe
            # esta en el PATH del sistema; no se necesita shell=True.
            result = subprocess.run(
                ["nordvpn", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                shell=False,
            )
            if result.returncode == 0:
                logger.info("✓ NordVPN CLI detectado")
                return "cli"
        except Exception:
            pass

        # Verificar app de escritorio en el registro de Windows
        if _WINREG_AVAILABLE:
            try:
                key = winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
                    0,
                    winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
                )
                i = 0
                while True:
                    try:
                        subkey_name = winreg.EnumKey(key, i)
                        subkey = winreg.OpenKey(key, subkey_name)
                        display_name = winreg.QueryValueEx(subkey, "DisplayName")[0]
                        if "NordVPN" in display_name:
                            logger.info("✓ NordVPN App de escritorio detectada")
                            return "app"
                        i += 1
                    except OSError:
                        break
                winreg.CloseKey(key)
            except Exception:
                pass

        logger.warning("⚠️ NordVPN no detectado, usando modo manual")
        return "manual"

    def _detect_original_ip(self):
        """Captura la IP original antes de conectar VPN."""
        try:
            self.original_ip = self.get_current_ip()
            logger.info(f"IP original: {self.original_ip}")
        except Exception:
            self.original_ip = None
            logger.warning("⚠️ No se pudo detectar IP original")

    # ── CONEXIÓN ──────────────────────────────────────────────────────────────

    def connect(self, country: Optional[str] = None) -> bool:
        """
        Conecta a NordVPN.

        Args:
            country: Código de país ('US', 'DE', etc.) o None para aleatorio.

        Returns:
            True si la conexión fue exitosa.
        """
        if country is None:
            country = random.choice(list(self.COUNTRY_NAMES.keys()))

        country_name = self.COUNTRY_NAMES.get(country, country)
        logger.info(f"Conectando a {country_name} ({country})...")

        if self.method == "cli":
            return self._connect_via_cli(country_name)
        elif self.method == "app":
            return self._connect_via_app(country_name)
        else:
            return self._connect_manual(country_name)

    def connect_or_raise(self, country: Optional[str] = None) -> None:
        """
        Igual que connect() pero lanza excepción si falla.
        Útil en tareas Celery donde un fallo debe propagar error.
        """
        if not self.connect(country):
            raise ConnectionError(
                f"No se pudo conectar a NordVPN "
                f"(método={self.method}, país={country})"
            )

    def _connect_via_cli(self, country: str) -> bool:
        """Conecta usando NordVPN CLI."""
        try:
            # Desconectar primero
            # [FIX v2.4] shell=False con lista de args. El pais viene de
            # settings.VPN_COUNTRIES (lista controlada), nunca de input usuario.
            # Aun asi, usar lista es la practica correcta.
            subprocess.run(
                ["nordvpn", "-d"],
                capture_output=True,
                timeout=30,
                shell=False,
            )
            time.sleep(3)

            # Conectar
            logger.info(f"Conectando CLI a {country}...")
            result = subprocess.run(
                ["nordvpn", "-c", "-g", country],
                capture_output=True,
                text=True,
                timeout=60,
                shell=False,
            )

            connected = (
                result.returncode == 0
                or "connected" in result.stdout.lower()
            )

            if connected:
                # [FIX v2.3] Cerrar popup "¿Pausar la conexion automatica?"
                # que aparece cada vez que NordVPN cambia de servidor
                self._dismiss_nordvpn_popup()
                time.sleep(10)
                new_ip = self.get_current_ip()
                if new_ip != self.original_ip and new_ip != "Unknown":
                    self.current_server = country
                    self.current_ip = new_ip
                    self.connection_count = 0
                    logger.success(f"✓ Conectado a {country} — IP: {new_ip}")
                    return True
                else:
                    logger.error("✗ VPN CLI conectó pero IP no cambió")
                    return False
            else:
                logger.error(f"✗ Error CLI: {result.stderr.strip()}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("✗ Timeout al conectar VPN CLI (60s)")
            return False
        except Exception as e:
            logger.error(f"✗ Error CLI: {e}")
            return False

    def _dismiss_nordvpn_popup(self):
        """
        [FIX v2.3] Cierra el popup que NordVPN muestra cada vez que cambia
        de servidor: "Pausar la conexion automatica en esta sesion?"
        Usa PowerShell para encontrar la ventana y enviar ESC (Cancelar).
        Sin dependencias externas (solo subprocess + PowerShell nativo).
        """
        try:
            ps_script = (
                "Add-Type -AssemblyName System.Windows.Forms; "
                "$proc = Get-Process -Name 'NordVPN' -ErrorAction SilentlyContinue; "
                "if ($proc) { "
                "  $wsh = New-Object -ComObject WScript.Shell; "
                "  $wsh.AppActivate('NordVPN'); "
                "  Start-Sleep -Milliseconds 500; "
                "[System.Windows.Forms.SendKeys]::SendWait('{ESC}'); "
                "}"
            )
            subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
                capture_output=True,
                timeout=5,
            )
        except Exception:
            pass  # No critico, continuar aunque falle

    def _connect_via_app(self, country: str) -> bool:
        """
        La app de escritorio de NordVPN no tiene API pública en Windows.
        Delega a manual (con protección contra bloqueo).
        """
        logger.warning("⚠️ NordVPN app de escritorio no tiene API pública, usando manual")
        return self._connect_manual(country)

    def _connect_manual(self, country: str) -> bool:
        """
        ✅ FIX: input() solo se llama si interactive=True.
        En modo Celery (interactive=False) lanza excepción explicativa.
        """
        if not self.interactive:
            # ✅ FIX: No bloquear el worker de Celery
            raise RuntimeError(
                f"Conexión VPN manual requerida para {country}, pero "
                "interactive=False (modo Celery). "
                "Conecta NordVPN manualmente antes de iniciar el worker, "
                "o establece VPN_ENABLED=False en .env."
            )

        # Modo consola interactiva
        print("\n" + "=" * 60)
        print("🔐 CONEXIÓN MANUAL REQUERIDA")
        print("=" * 60)
        print(f"\n  1. Abre la aplicación NordVPN")
        print(f"  2. Conecta manualmente a: {country}")
        print(f"  3. Espera a que la conexión se establezca")
        print(f"\n👉 Presiona ENTER cuando estés conectado...")
        print("=" * 60 + "\n")
        input()

        time.sleep(2)
        new_ip = self.get_current_ip()

        if new_ip != self.original_ip and new_ip != "Unknown":
            self.current_server = country
            self.current_ip = new_ip
            self.connection_count = 0
            logger.success(f"✓ VPN manual verificada — IP: {new_ip}")
            return True

        logger.error("✗ VPN no detectada")
        print(f"\n⚠️ IP no cambió: actual={new_ip}  original={self.original_ip}")
        print("¿Continuar de todas formas? (s/n): ")
        return input().strip().lower() == "s"

    # ── DESCONEXIÓN ───────────────────────────────────────────────────────────

    def disconnect(self) -> None:
        """Desconecta la VPN."""
        try:
            if self.method == "cli":
                # [FIX v2.4] shell=False
                subprocess.run(
                    ["nordvpn", "-d"],
                    capture_output=True,
                    timeout=30,
                    shell=False,
                )
                logger.info("✓ VPN desconectada (CLI)")
            elif self.interactive:
                print("\n" + "=" * 60)
                print("🔓 DESCONEXIÓN MANUAL REQUERIDA")
                print("=" * 60)
                print("\n  1. Abre la aplicación NordVPN")
                print("  2. Haz clic en 'Disconnect'")
                print("\n👉 Presiona ENTER cuando hayas desconectado...")
                input()

            self.current_server = None
            self.current_ip = None

        except Exception as e:
            logger.warning(f"Error al desconectar VPN: {e}")

    # ── ROTACIÓN ──────────────────────────────────────────────────────────────

    def rotate(self, avoid_current: bool = True) -> bool:
        """
        Rota la conexión a un servidor diferente.

        Args:
            avoid_current: Si True, evita el país actual.
        """
        logger.info("🔄 Rotando VPN...")
        self.disconnect()
        time.sleep(5)

        available = list(self.COUNTRY_NAMES.keys())
        if avoid_current and self.current_server:
            # Buscar código del servidor actual
            curr_code = next(
                (k for k, v in self.COUNTRY_NAMES.items() if v == self.current_server),
                None,
            )
            if curr_code and curr_code in available:
                available.remove(curr_code)

        new_country = random.choice(available) if available else random.choice(
            list(self.COUNTRY_NAMES.keys())
        )

        success = self.connect(new_country)
        if success:
            logger.success(f"✓ Rotación exitosa → {self.COUNTRY_NAMES[new_country]}")
        else:
            logger.error("✗ Falló la rotación de VPN")
        return success

    def auto_rotate_if_needed(self) -> bool:
        """Rota automáticamente si se alcanzó el límite de conexiones."""
        self.connection_count += 1
        if self.connection_count >= self.max_connections_per_server:
            logger.warning(
                f"⚠️ Límite alcanzado ({self.max_connections_per_server} conexiones)"
            )
            return self.rotate()
        return False

    # ── VERIFICACIÓN ──────────────────────────────────────────────────────────

    # ── VERIFICACION ─────────────────────────────────────────────────────────

    def get_current_ip(self) -> str:
        """
        Obtiene la IP publica actual.
        [FIX v2.3] Cache de 30s: evita que multiples threads simultaneos saturen
                   los servicios externos y devuelvan timeout -> 'Unknown'.
        """
        with self._ip_cache_lock:
            now = time.time()
            if now - self._ip_cache_time < self._ip_cache_ttl and self._ip_cache_value != "Unknown":
                return self._ip_cache_value   # devolver caché fresco

        # Fuera del lock para no bloquear otros threads mientras hace HTTP
        services = [
            "https://api.ipify.org?format=text",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
            "https://ipinfo.io/ip",
            "https://checkip.amazonaws.com",
        ]
        result = "Unknown"
        for service in services:
            try:
                resp = requests.get(service, timeout=8)
                if resp.status_code == 200:
                    result = resp.text.strip()
                    break
            except Exception:
                continue

        with self._ip_cache_lock:
            self._ip_cache_value = result
            self._ip_cache_time  = time.time()

        if result == "Unknown":
            logger.warning("⚠️ No se pudo obtener IP actual")
        return result

    def verify_vpn_active(self) -> bool:
        """
        Verifica que la IP actual sea diferente a la original.

        [FIX v2.3] Lógica corregida:
          - original_ip='Unknown' → no hay baseline, asumir activa
          - current='Unknown'    → no podemos verificar, asumir activa
            (mejor falso positivo que reconectar 10 veces simultáneamente)
          - current == original_ip → VPN inactiva (IP real expuesta)
        """
        # Sin baseline no podemos comparar → asumir activa
        if not self.original_ip or self.original_ip == "Unknown":
            logger.warning("⚠️ IP original desconocida, asumiendo VPN activa")
            return True

        current = self.get_current_ip()
        self.current_ip = current

        # No pudimos consultar IP externa → no asumir caída
        if current == "Unknown":
            logger.warning("⚠️ No se pudo verificar IP — asumiendo VPN activa para no reconectar en falso")
            return True

        if current != self.original_ip:
            logger.info(f"✓ VPN activa — IP: {current}")
            return True

        logger.warning(f"⚠️ VPN inactiva | IP={current} == original={self.original_ip}")
        return False

    def reconnect_if_disconnected(self) -> bool:
        """Reconecta si la VPN se cayó."""
        if not self.verify_vpn_active():
            logger.warning("🔄 VPN caída, reconectando...")
            return self.connect(self.current_server)
        return True

    # ── ESTADO ────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict:
        """Estado completo de la VPN."""
        is_active = self.verify_vpn_active()
        return {
            "method":           self.method,
            "interactive":      self.interactive,
            "connected":        is_active,
            "server":           self.current_server,
            "current_ip":       self.current_ip,
            "original_ip":      self.original_ip,
            "connection_count": self.connection_count,
            "max_connections":  self.max_connections_per_server,
        }

    def print_status(self):
        """Imprime el estado de forma legible."""
        s = self.get_status()
        print("\n" + "=" * 60)
        print("📊 ESTADO DE VPN")
        print("=" * 60)
        print(f"  Método:      {s['method']}")
        print(f"  Conectada:   {'✓ Sí' if s['connected'] else '✗ No'}")
        print(f"  Servidor:    {s['server'] or 'N/A'}")
        print(f"  IP actual:   {s['current_ip'] or 'N/A'}")
        print(f"  IP original: {s['original_ip'] or 'N/A'}")
        print(f"  Conexiones:  {s['connection_count']}/{s['max_connections']}")
        print("=" * 60 + "\n")

    # ── CONTEXT MANAGER ───────────────────────────────────────────────────────

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        """✅ FIX: Seguro aunque __init__ haya fallado a medias."""
        try:
            method = getattr(self, "method", None)
            if method == "cli":
                self.disconnect()
        except Exception:
            pass


# ── TEST STANDALONE ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logger.add(sys.stdout, colorize=True)
    logger.add("vpn_windows_test.log", rotation="10 MB")

    print("\n" + "=" * 60)
    print("   NORDVPN MANAGER — TEST WINDOWS 11")
    print("=" * 60 + "\n")

    # interactive=True solo para prueba manual
    vpn = NordVPNManagerWindows(method="auto", interactive=True)
    vpn.print_status()

    print("Iniciando test de conexión a US...\n")
    if vpn.connect("US"):
        vpn.print_status()
        print("\nRotando a DE...\n")
        vpn.rotate()
        vpn.print_status()
        vpn.disconnect()
    else:
        print("✗ Test de conexión falló")

    print("=" * 60)
    print("   TEST COMPLETADO")
    print("=" * 60 + "\n")
