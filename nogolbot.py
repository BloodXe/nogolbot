"""
NoGolBot - Analizador de partidos en vivo (CUALQUIER liga del mundo)
para el mercado "No hay mas goles en los ultimos X minutos"

FUENTE DE DATOS: SofaScore (via la libreria no oficial `pysofascore`),
endpoint /sport/football/events/live. Esa sola llamada devuelve TODOS
los partidos en vivo de TODAS las ligas del mundo en una sola solicitud.


QUE HACE:
  1. Revisa todos los partidos en vivo del mundo (intervalo dinamico,
     ver mas abajo).
  2. Para los que estan en el tramo final (minuto configurable),
     consulta ADEMAS sus estadisticas reales (tiros a puerta, tiros
     totales, grandes ocasiones, xG) y calcula la probabilidad de que
     NO entre ningun gol mas, con un modelo de Poisson ajustado por
     el ritmo combinado de todas esas señales (cada una pesada segun
     PESOS_SEÑALES). Si SofaScore no trae alguna señal para un partido
     puntual, el modelo se ajusta solo con las que sí tiene.
  3. Si la probabilidad supera un umbral, imprime una alerta por
     CONSOLA (opcionalmente redactada por una IA local o por API) para
     que TU decidas si apuestas. El bot NO coloca apuestas reales ni
     manda notificaciones fuera de la terminal.

Instala dependencias y corre:
  pip install pysofascore requests
  python nogolbot.py

Variable de entorno opcional (solo si vas a usar MODO_IA = "api"):
  export GEMINI_API_KEY="tu-clave-real"

IMPORTANTE: herramienta de apoyo estadistico, no garantia de ganar.
Define un limite de dinero que estes dispuesto a perder y no lo superes.
Ningun ajuste de este modelo elimina el riesgo de las apuestas: en el
mejor de los casos, te ayuda a medir tu edge real (ver "Calibracion y
registro de resultados" mas abajo) y a no apostar en señales que en la
practica no lo tienen.

===============================================================
CAMBIOS - REVISION 1 (ver conversacion para el detalle):
  1. BUG CRITICO: antes, al cruzar el minuto 90 el modelo trataba el
     partido como terminado y devolvia 100% de "no mas goles" de
     forma instantanea. Ahora se asume un descuento de la 2da parte
     (DESCUENTO_2T_ASUMIDO_DEFAULT) y nunca se llega a una certeza
     absoluta mientras el partido siga "inprogress".
  2. SESGO: los goles NO se reparten parejo en los 90'; el tramo
     75-90'+ concentra ~20-26% de los goles segun varios estudios
     (ver FACTOR_INTENSIDAD_TRAMO_FINAL), muy por encima del 16.7%
     "parejo". Se agrega un factor de inflacion para ese tramo.
  3. BUG: _buscar_metrica devolvia el PRIMER item que matcheaba y
     cortaba ahi. Si SofaScore no trae el periodo "ALL" y separa
     1er/2do tiempo, se perdia en silencio la mitad de la estadistica.
     Ahora se suman todos los items que matchean.
  4. BUG: el calculo de minuto no distinguia tiempo extra (prorroga)
     de tiempo regular, pudiendo dar un minuto muy por debajo del
     real en partidos de copa. Ahora se detecta y se omite el
     partido (se prefiere no vigilarlo a vigilarlo con un minuto
     erroneo).
  5. NUEVA SEÑAL: tarjetas rojas (hay evidencia academica de que el
     goleo total tiende a subir tras una expulsion).
  6. NUEVA HERRAMIENTA: valor_esperado_apuesta(), para comparar la
     probabilidad del modelo contra la cuota real de tu casa de
     apuestas (la unica forma real de saber si una apuesta tiene
     valor, un umbral fijo de probabilidad no alcanza).

CAMBIOS - REVISION 2 (revision completa de logica + robustez):
  7. NUEVO (el mas importante): registro de resultados en disco
     (ARCHIVO_HISTORIAL) para cada alerta emitida, con resolucion
     automatica ("acierto"/"fallo") en ciclos posteriores. Sin esto
     era imposible saber si los umbrales (65%/85%) estan bien
     calibrados. Ver resumen_calibracion() para analizarlo.
  8. NUEVO: valor_esperado_apuesta() ahora tambien devuelve una
     sugerencia de tamaño de apuesta (Kelly fraccionado + tope duro),
     para que el riesgo por señal escale con el edge real y no sea
     el mismo monto para una señal fuerte que para una al limite.
  9. FIX: USAR_MOMENTUM=True sin agregar "momentum" a PESOS_SEÑALES
     gastaba una solicitud extra a SofaScore por partido sin ningun
     efecto en el modelo. Ahora se valida al arrancar y se avisa/
     corrige solo (_validar_configuracion).
  10. FIX: _resumen_confianza_señales usaba una lista de señales fija
      a mano, desincronizada de PESOS_SEÑALES (por eso no contaba
      "momentum" aunque estuviera activo). Ahora es dinamica: se basa
      directamente en las claves de PESOS_SEÑALES.
  11. NUEVA SEÑAL OPCIONAL: corners (tiros de esquina). Se extrae
      siempre (mismo llamado, sin costo extra) pero queda sin peso
      por default - mismo patron "opt-in" que momentum: agregala a
      PESOS_SEÑALES vos mismo despues de confirmar la clave real con
      imprimir_estadisticas_crudas().
  12. ROBUSTEZ: estado persistente en disco (partidos_ya_avisados,
      cache de 404) para sobrevivir a un reinicio sin volver a avisar
      partidos ya vistos; poda automatica de esas caches para que no
      crezcan sin limite en sesiones largas; backoff exponencial
      cuando SofaScore empieza a fallar seguido (en vez de reintentar
      agresivo, lo cual empeora el riesgo de bloqueo de IP).
  13. FIX DE SEGURIDAD: GEMINI_API_KEY ahora se lee de una variable de
      entorno en vez de quedar hardcodeada en el archivo.
  14. PRINTS: resumen del ciclo partido en varias lineas cortas en vez
      de una sola linea densa; las lineas de detalle por partido ya no
      se repiten cada 2 minutos si la probabilidad no se movio de
      forma significativa desde la ultima vez que se imprimieron.
===============================================================
"""

import csv
import json
import math
import os
import time
import requests
from datetime import datetime

from sofascore_api import SofaScoreClient
from sofascore_api.client import SofaScoreError

# ============== CONFIG ==============

# Minuto a partir del cual vigilamos el partido para el mercado
# "no mas goles" (75 = ultimos 15 min, 80 = ultimos 10 min)
MINUTO_INICIO_VIGILANCIA = 75

# Minuto HASTA el cual nos sigue interesando el partido (inclusive).
# Pasado este minuto, en la practica tu casa de apuestas ya achico la
# cuota de "no mas goles" a un punto donde no vale la pena -el mercado
# ya "vio" lo mismo que ve este modelo (marcador + tiempo restante)-,
# asi que dejamos de pedirle estadisticas a SofaScore y de alertar
# sobre ese partido aunque siga en vivo. Si con tu casa de apuestas
# notas que las cuotas siguen siendo buenas mas alla de este minuto,
# subilo.
MINUTO_FIN_VIGILANCIA = 82

# Si el minuto calculado para un partido "inprogress" (sin que la
# descripcion indique tiempo extra) supera este valor, se descarta el
# dato en vez de usarlo. Un partido regular con VAR, lesiones y todo
# el descuento moderno del mundo no llega a esto; si el numero da mas
# alto es casi seguro un timestamp viejo/atascado de SofaScore (comun
# en ligas amateur de cobertura floja: ver los casos min 114'/119'/126'
# en partidos amateur de Portugal/Serbia detectados en produccion).
# Preferimos perder esa señal a arriesgar una alerta de alta confianza
# sobre un minuto que no representa el partido real.
MINUTO_MAXIMO_CONFIABLE = 105

# Probabilidad minima (0-1) para que te avisemos
UMBRAL_PROBABILIDAD = 0.65

# Umbral MAS EXIGENTE, aplicado en vez del umbral normal cuando un
# partido no trae NINGUNA señal de ritmo extra (0/4: solo goles+minuto,
# ver _resumen_confianza_señales). Con el umbral base, casi cualquier
# 0-0/1-0 amateur en el minuto 85+ lo cruza -no porque el partido sea
# especial, sino porque "pocos goles + poco tiempo" ya empuja la
# probabilidad para arriba incluso sin ninguna informacion adicional-.
# Esa es la señal mas facil de que YA este reflejada en la cuota del
# mercado (ver _resumen_confianza_señales), asi que le pedimos mas
# margen antes de considerarla una alerta accionable. Ajustalo si
# preferis ser mas o menos estricto.
UMBRAL_PROBABILIDAD_BAJA_CONFIANZA = 0.75

# --- Intervalo DINAMICO (en vez de uno fijo) ---
# Con un intervalo fijo de 15 min, un partido puede "aparecer" por
# primera vez en el minuto 88, cuando ya no queda valor en la apuesta.
# Para evitarlo: revisamos rápido SOLO cuando hay un partido cerca del
# final, y lento el resto del tiempo (para no golpear SofaScore de mas,
# ver aviso arriba sobre riesgo de bloqueo de IP).

# Intervalo cuando NINGÚN partido está cerca de terminar
INTERVALO_LARGO = 900  # 15 min

# Intervalo cuando SÍ hay al menos un partido acercándose/dentro de la
# ventana final (aquí sí necesitamos precisión)
INTERVALO_CORTO = 120  # 2 min

# Minuto desde el cual empezamos a vigilar de cerca (más temprano que
# MINUTO_INICIO_VIGILANCIA, para "adelantarnos" y no llegar tarde)
MINUTO_ALERTA_TEMPRANA = 65

# Promedio de goles por partido (90 min) para ligas de las que no
# tenemos un promedio especifico. Como ahora cubrimos cualquier liga
# del mundo (muchas con menos datos historicos disponibles), este
# valor generico es el que mas se va a usar.
PROMEDIO_GOLES_LIGA_DEFAULT = 2.7

# --- Señales estadísticas del modelo ---
# El modelo original solo miraba goles + tiros a puerta. Ahora combina
# varias señales, cada una comparando "lo real" vs "lo esperado para
# este minuto", y las promedia con estos pesos (deben sumar 1.0 si
# querés que la escala se mantenga igual; si falta una señal para un
# partido puntual, se renormaliza automáticamente entre las que sí
# están disponibles).
PESOS_SEÑALES = {
    "goles": 0.30,              # el resultado real, siempre disponible
    "tiros_a_puerta": 0.25,     # ocasiones claras de gol
    "tiros_totales": 0.15,      # volumen de ataque en general
    "grandes_ocasiones": 0.20,  # "Big chances" de SofaScore: ocasiones de alta probabilidad
    "xg": 0.10,                 # goles esperados acumulados (calidad, no solo cantidad)
}

# Promedios combinados (ambos equipos) por cada 90 minutos, usados como
# referencia "esperada" para cada señal, igual que PROMEDIO_GOLES_LIGA_DEFAULT.
# Son valores genéricos de fútbol amateur/profesional promedio; ajustalos
# si notás que el modelo se dispara para una liga en particular.
PROMEDIO_TIROS_A_PUERTA_90 = 10.0
PROMEDIO_TIROS_TOTALES_90 = 24.0
PROMEDIO_GRANDES_OCASIONES_90 = 5.0
# xG total esperado en 90' se aproxima al promedio de goles de la liga,
# así que reusamos PROMEDIO_GOLES_LIGA_DEFAULT como su referencia.
PROMEDIO_CORNERS_90 = 10.0  # solo se usa si agregás "corners" a PESOS_SEÑALES (ver USAR_MOMENTUM más abajo)

# --- Descuento (tiempo añadido) ---
# Cuanto tiempo asumimos que dura el descuento de la 2da parte cuando
# el minuto calculado ya pasó de 90 y SofaScore todavía no marcó el
# partido como "finished". Desde la directiva de IFAB (adoptada en el
# Mundial 2022 y luego en ligas como la Premier League) los árbitros
# añaden TODO el tiempo perdido: los partidos de Premier League
# promediaron ~100-101 minutos reales en 2022-24 (~10-11' añadidos
# entre ambas partes), bajando algo desde el ajuste 2024-25 sobre
# celebraciones de gol. En ligas menores el descuento suele ser mas
# corto. Este valor es una aproximacion conservadora pensada para NO
# declarar el partido "terminado" antes de tiempo; ajustalo si notas
# que tu liga en particular usa descuentos mas largos o mas cortos.
DESCUENTO_2T_ASUMIDO_DEFAULT = 6

# --- Intensidad del tramo final ---
# Los goles NO se reparten parejo en los 90': multiples estudios
# (ligas top europeas, Mundiales, Copa Libertadores) muestran que el
# tramo final (75-90'+) concentra entre ~20% y ~26% de los goles
# totales de un partido, por encima del 16.7% que le tocaria si la
# tasa fuese constante en el tiempo. Sin este ajuste, el modelo
# SUBESTIMA el riesgo de gol justo en el tramo que vigila este bot, y
# por lo tanto SOBREESTIMA la probabilidad de "no mas goles".
# Fuente principal (metodologia mas rigurosa, ligas top europeas):
# Alberti et al. 2013, "Goal scoring patterns in major European
# soccer leagues", Sport Sciences for Health — encontraron 20.2% de
# los goles en el tramo 75-90' (vs. 16.7% esperado si fuera parejo).
# Otros estudios en ligas sudamericanas encuentran hasta 25-26%. Se
# usa un valor intermedio y conservador; es una aproximacion, no una
# calibracion exacta por liga.
FACTOR_INTENSIDAD_TRAMO_FINAL = 1.25
MINUTO_INICIO_TRAMO_FINAL = 75

# --- Tarjetas rojas ---
# Evidencia academica (ej. estudios en las 5 grandes ligas europeas y
# en Mundiales) muestra que tras una expulsion el equipo con
# superioridad numerica aumenta su tasa de gol, y que el goleo TOTAL
# del partido tiende a subir (el aumento del equipo con un jugador
# mas suele pesar mas que la caida del equipo con uno menos). Este
# factor es una aproximacion direccional, no un coeficiente calibrado
# con precision: ajustalo si te parece muy fuerte o muy debil.
FACTOR_TARJETA_ROJA = 1.15

# Señal experimental: intensidad de "ataque/presión" reciente via el
# gráfico de momentum de SofaScore (lo más parecido a "ataques
# peligrosos" que expone su API, ya que no tiene ese stat como tal).
# La dejamos APAGADA por default porque no pude verificar en vivo la
# escala real de estos valores (ver obtener_intensidad_momentum). Si la
# activás, agregá manualmente una clave "momentum" a PESOS_SEÑALES
# arriba (restando ese peso de las demás para que sigan sumando 1.0)
# después de confirmar con imprimir_estadisticas_crudas() que los
# números que da tienen sentido para vos.
USAR_MOMENTUM = False

# Corners (tiros de esquina): SofaScore SI publica este stat de forma
# estandar (a diferencia de "ataques peligrosos", que no existe como
# contador - ver USAR_MOMENTUM arriba). Se extrae siempre en
# obtener_estadisticas_partido sin costo extra (viene en la misma
# respuesta que tiros/xG), pero queda SIN PESO por default: la
# evidencia que vincula corners con "va a entrar otro gol" es mas
# debil que la de tiros a puerta/grandes ocasiones/xG, asi que no
# quiero que cambie el modelo sin que vos lo decidas a proposito.
# Para activarla: confirma la clave real con
# imprimir_estadisticas_crudas() y despues agregá algo como
# "corners": 0.05 a PESOS_SEÑALES (restando ese peso de las demas
# para que sigan sumando 1.0).

# --- Gestion de banca (Kelly fraccionado) ---
# Kelly COMPLETO es matematicamente optimo solo si tu estimacion de
# probabilidad es exacta - la nuestra es una aproximacion heuristica,
# no una calibrada con datos reales (todavia; ver ARCHIVO_HISTORIAL
# mas abajo), asi que usar Kelly completo seria apostar como si el
# modelo fuera mas confiable de lo que hoy se puede demostrar. Se usa
# una fraccion conservadora del Kelly completo, ademas de un tope duro
# que nunca se cruza pase lo que pase.
FRACCION_KELLY = 0.25  # 1/4 de Kelly: estandar conservador cuando hay incertidumbre sobre el edge real
TOPE_FRACCION_BANCA_POR_APUESTA = 0.05  # nunca sugerir mas del 5% de la banca en una sola señal

# --- Registro de resultados y calibracion ---
# El problema mas grande de un modelo "a mano" como este es que no hay
# forma de saber si el 65%/85% de UMBRAL_PROBABILIDAD* representan de
# verdad esos porcentajes de aciertos en la realidad. Cada alerta se
# guarda aca, y en ciclos posteriores se resuelve sola comparando el
# marcador antes/despues (ver actualizar_resultados_pendientes). Con
# resumen_calibracion() podes comparar, tramo por tramo de probabilidad,
# lo que dijo el modelo contra lo que paso en la realidad.
ARCHIVO_HISTORIAL = "historial_alertas.csv"
COLUMNAS_HISTORIAL = [
    "match_id", "fecha_hora_alerta", "timestamp_alerta", "competicion",
    "local", "visita", "goles_local_alerta", "goles_visita_alerta",
    "minuto_alerta", "prob_modelo", "umbral_usado", "n_señales_extra",
    "n_señales_posibles", "resultado", "goles_local_final",
    "goles_visita_final", "minuto_resolucion", "fecha_hora_resolucion",
]

# Minutos de RELOJ REAL que se esperan, desde que se emitio una alerta,
# antes de asumir "acierto" para un partido que desaparecio de la lista
# de partidos en vivo sin que se le haya detectado un gol de mas. OJO
# con la limitacion: esto NO confirma el marcador final con una consulta
# aparte (para no gastar solicitudes extra a SofaScore); si el partido
# sale de la lista de "en vivo" sin haber mostrado un gol adicional
# mientras lo pudimos seguir viendo, se asume acierto. Es una
# aproximacion razonable (el mercado que vigilamos es "no mas goles",
# y esa es la señal directa que necesitamos), pero no es 100% infalible.
MINUTOS_ESPERA_CONFIRMAR_ACIERTO = 25

# --- Persistencia y limpieza de caches en memoria ---
# Sin esto, un reinicio del bot pierde toda la memoria de que partidos
# ya se avisaron o no tienen stats, y podrias recibir alertas duplicadas
# de un partido que ya habias visto antes del reinicio.
ARCHIVO_ESTADO = "nogolbot_estado.json"

# Ningun partido real sigue "en vivo" tantas horas: pasado este tiempo,
# el id ya no puede volver a aparecer legitimamente, asi que se puede
# olvidar sin riesgo. Evita que las caches en memoria/disco crezcan sin
# limite en sesiones que corren dias o semanas seguidas.
TIEMPO_MAXIMO_RETENCION_CACHE = 6 * 3600  # 6 horas

# No repetir la linea de detalle de un partido si su probabilidad no se
# movio al menos esto desde la ultima vez que se imprimio (evita 6-8
# lineas casi identicas del mismo partido mientras espera en la ventana).
UMBRAL_CAMBIO_PROB_PARA_REIMPRIMIR = 0.02

# --- Backoff ante fallos repetidos de SofaScore ---
# Si SofaScore empieza a devolver errores seguidos (posible bloqueo de
# IP, caida del servicio, etc.), reintentar cada INTERVALO_CORTO/LARGO
# de forma agresiva solo empeora el riesgo de bloqueo. En vez de eso, el
# tiempo de espera se duplica en cada fallo consecutivo hasta este tope.
BACKOFF_MAXIMO_SEGUNDOS = 1800  # 30 min

# --- Configuracion del cliente de SofaScore ---
# Si SofaScore te empieza a bloquear (errores 403 repetidos), podes
# configurar un proxy aca, ej: "http://user:pass@host:puerto"
SOFASCORE_PROXY = None
SOFASCORE_TIMEOUT = 15
SOFASCORE_RETRIES = 3

# --- Capa de IA que redacta el veredicto (opcional) ---
# "ninguno" -> solo el numero, sin IA
# "local"   -> usa Ollama corriendo en tu PC (100% gratis, sin internet)
# "api"     -> usa Google Gemini API (capa gratuita real, necesita internet)
MODO_IA = "ninguno"

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.1:8b"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AQ.Ab8RN6IJP62kjtcxl1svvLvDlN0S_g2-ASKjFZ6Qoilq8Sgrxg")
GEMINI_MODEL = "gemini-2.0-flash-lite"

# Que tan detallado es el log de cada ciclo. Antes, MOSTRAR_DETALLE=True
# imprimia UNA LINEA POR CADA PARTIDO EN VIVO DEL MUNDO (con 492
# partidos en vivo en un momento dado, eso son 492 lineas por ciclo,
# la enorme mayoria sin ninguna utilidad: partidos en el minuto 6, 20,
# 24... que todavia estan a una hora de siquiera acercarse al mercado
# que vigilamos). Ahora MOSTRAR_DETALLE solo imprime linea por linea
# los partidos que de verdad importan (los que ya estan cerca o dentro
# de la ventana de vigilancia); todo lo demas se resume en un solo
# renglon de conteo al final del ciclo. Los partidos sin minuto
# reportado (entretiempo sin confirmar, datos incompletos, etc.) nunca
# se imprimen uno por uno: no aportan nada y tampoco se analizan.
MOSTRAR_DETALLE = True

# Poné esto en True solo para debug puntual si necesitás ver TODOS los
# partidos en vivo del mundo, uno por uno, incluidos los que están muy
# lejos de la ventana de vigilancia. Genera mucho ruido en consola;
# no se recomienda dejarlo prendido en uso normal.
MOSTRAR_TODOS_LOS_PARTIDOS_LEJANOS = False

# Partidos para los que ya se emitió una alerta (no se vuelve a avisar,
# y tampoco se les vuelve a consultar tiros a puerta: ya cumplieron su
# función y seguir pidiendo sus stats solo genera trafico de mas).
# Dict match_id(str) -> timestamp de la alerta (en vez de un set simple)
# para poder: (a) persistir a disco entre reinicios y (b) podar entradas
# viejas (ver _purgar_cache_vencido) sin que esto crezca sin limite en
# sesiones largas.
partidos_ya_avisados = {}

# Contador de solicitudes hechas a SofaScore en esta sesión (solo
# informativo: a diferencia de API-Football, SofaScore no documenta un
# limite diario, asi que esto no bloquea nada, solo te deja ver cuanto
# trafico esta generando el bot).
_contador_solicitudes_sesion = 0

# Contador de partidos para los que SofaScore respondio 404 al pedir
# estadisticas (osea: NO publica tiros/xG/etc. para ese partido, algo
# muy comun en ligas amateur o de cobertura floja). Esto NO es un error
# del bot -el modelo ya sabe seguir funcionando solo con el marcador
# cuando no hay estadisticas- asi que ya no se imprime linea por linea
# (ver obtener_estadisticas_partido): se cuenta aca y se resume al final
# de cada ciclo para que sigas teniendo visibilidad sin el ruido.
_contador_sin_stats_404_sesion = 0

# Partidos para los que SofaScore YA respondio 404 al pedir estadisticas
# en esta sesion. Es muy poco probable que un partido que no tiene
# estadisticas detalladas al minuto 75 de repente empiece a tenerlas 2
# minutos despues (la cobertura de una liga no cambia a mitad de
# partido), asi que evitamos volver a pedirlas en cada ciclo mientras
# el partido siga en la ventana de vigilancia: eso es trafico de mas a
# SofaScore (ver aviso de scraping no oficial arriba) que solo iba a
# terminar en el mismo 404 de nuevo. Si algun caso puntual SÍ llegara a
# publicarlas mas tarde, el efecto es identico al que ya tenias antes de
# este cambio (el modelo sigue sin usarlas para ese partido).
# Dict match_id(str) -> timestamp, mismo motivo que partidos_ya_avisados.
_partidos_sin_stats_conocidos = {}

# Ultima probabilidad IMPRESA (no la ultima calculada) para cada partido
# en la ventana de vigilancia, usada solo para no repetir la linea de
# detalle en consola cuando la probabilidad practicamente no se movio
# desde el ciclo anterior (ver UMBRAL_CAMBIO_PROB_PARA_REIMPRIMIR).
# Dict match_id(str) -> (probabilidad, timestamp).
_ultima_prob_mostrada = {}

# Contador de fallos consecutivos consultando SofaScore (se resetea a 0
# apenas una solicitud funciona). Controla el backoff exponencial en
# main() - ver BACKOFF_MAXIMO_SEGUNDOS.
_errores_consecutivos_sofascore = 0

# Cliente HTTP de SofaScore (se reutiliza la misma sesión entre ciclos)
_client = SofaScoreClient(
    proxy=SOFASCORE_PROXY,
    timeout=SOFASCORE_TIMEOUT,
    retries=SOFASCORE_RETRIES,
)


def _registrar_solicitud():
    """Cuenta una solicitud hecha a SofaScore (solo para logging)."""
    global _contador_solicitudes_sesion
    _contador_solicitudes_sesion += 1
    return _contador_solicitudes_sesion


def _validar_configuracion():
    """
    Chequeos de configuracion que corren UNA vez al arrancar main(), para
    detectar combinaciones inconsistentes antes de que generen trafico
    inutil a SofaScore o fallos silenciosos durante horas de ejecucion.
    """
    global USAR_MOMENTUM

    if USAR_MOMENTUM and "momentum" not in PESOS_SEÑALES:
        print(
            f"[{datetime.now()}] ⚠️  AVISO DE CONFIGURACIÓN: USAR_MOMENTUM=True pero "
            f"'momentum' no tiene peso asignado en PESOS_SEÑALES. Esa señal se iba a "
            f"calcular (una solicitud extra a SofaScore por partido) y descartar sin "
            f"usarse en el modelo. Se desactiva USAR_MOMENTUM para esta sesión — "
            f"agregá 'momentum': <peso> a PESOS_SEÑALES (restando ese peso de las "
            f"demás) si de verdad querés activarla."
        )
        USAR_MOMENTUM = False

    if MODO_IA == "api" and not GEMINI_API_KEY:
        print(
            f"[{datetime.now()}] ⚠️  AVISO DE CONFIGURACIÓN: MODO_IA='api' pero no hay "
            f"GEMINI_API_KEY configurada (variable de entorno vacía). El veredicto de "
            f"IA se va a omitir en cada alerta hasta que la configures: "
            f"export GEMINI_API_KEY=\"tu-clave-real\"."
        )


def _cargar_estado():
    """
    Carga partidos_ya_avisados y _partidos_sin_stats_conocidos desde
    ARCHIVO_ESTADO (si existe), para que un reinicio del bot no pierda
    la memoria de que partidos ya se avisaron o no tienen stats.
    """
    global partidos_ya_avisados, _partidos_sin_stats_conocidos
    try:
        with open(ARCHIVO_ESTADO, "r", encoding="utf-8") as f:
            datos = json.load(f)
        partidos_ya_avisados = dict(datos.get("avisados", {}))
        _partidos_sin_stats_conocidos = dict(datos.get("sin_stats", {}))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[{datetime.now()}] Aviso: no se pudo cargar {ARCHIVO_ESTADO} ({e}), arrancando limpio.")


def _guardar_estado():
    """Guarda el estado en disco. Se llama al final de cada ciclo y al salir."""
    try:
        with open(ARCHIVO_ESTADO, "w", encoding="utf-8") as f:
            json.dump({
                "avisados": partidos_ya_avisados,
                "sin_stats": _partidos_sin_stats_conocidos,
            }, f)
    except Exception as e:
        print(f"[{datetime.now()}] Aviso: no se pudo guardar {ARCHIVO_ESTADO} ({e}).")


def _purgar_cache_vencido():
    """
    Elimina entradas mas viejas que TIEMPO_MAXIMO_RETENCION_CACHE de las
    caches en memoria, para que no crezcan sin limite en sesiones que
    corren dias o semanas seguidas. Se llama una vez por ciclo.
    """
    ahora = time.time()

    vencidos = [mid for mid, ts in partidos_ya_avisados.items() if ahora - ts > TIEMPO_MAXIMO_RETENCION_CACHE]
    for mid in vencidos:
        del partidos_ya_avisados[mid]

    vencidos = [mid for mid, ts in _partidos_sin_stats_conocidos.items() if ahora - ts > TIEMPO_MAXIMO_RETENCION_CACHE]
    for mid in vencidos:
        del _partidos_sin_stats_conocidos[mid]

    vencidos = [mid for mid, (_, ts) in _ultima_prob_mostrada.items() if ahora - ts > TIEMPO_MAXIMO_RETENCION_CACHE]
    for mid in vencidos:
        del _ultima_prob_mostrada[mid]


# ============== REGISTRO DE RESULTADOS Y CALIBRACION ==============
# Ver comentario de ARCHIVO_HISTORIAL en la seccion de CONFIG: sin esto
# es imposible saber si UMBRAL_PROBABILIDAD/UMBRAL_PROBABILIDAD_BAJA_
# CONFIANZA representan de verdad esos porcentajes de aciertos reales.

def _leer_historial(archivo=ARCHIVO_HISTORIAL):
    """Devuelve todas las filas del historial como lista de dicts (strings). [] si no existe."""
    try:
        with open(archivo, "r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def _escribir_historial(filas, archivo=ARCHIVO_HISTORIAL):
    """Reescribe el archivo completo (se usa al resolver alertas pendientes)."""
    with open(archivo, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS_HISTORIAL)
        writer.writeheader()
        writer.writerows(filas)


def _append_historial(fila, archivo=ARCHIVO_HISTORIAL):
    """Agrega una fila nueva sin reescribir el archivo (se usa al emitir una alerta)."""
    existe = os.path.exists(archivo)
    with open(archivo, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS_HISTORIAL)
        if not existe:
            writer.writeheader()
        writer.writerow(fila)


def registrar_alerta(match_id, contexto, prob, umbral_efectivo, confianza):
    """Guarda una alerta recien emitida en ARCHIVO_HISTORIAL con resultado 'pendiente'."""
    fila = {
        "match_id": str(match_id),
        "fecha_hora_alerta": datetime.now().isoformat(timespec="seconds"),
        "timestamp_alerta": time.time(),
        "competicion": contexto["competicion"],
        "local": contexto["local"],
        "visita": contexto["visita"],
        "goles_local_alerta": contexto["goles_local"],
        "goles_visita_alerta": contexto["goles_visita"],
        "minuto_alerta": contexto["minuto"],
        "prob_modelo": round(prob, 4),
        "umbral_usado": round(umbral_efectivo, 4),
        "n_señales_extra": confianza["n_extra"],
        "n_señales_posibles": confianza["n_extra_posibles"],
        "resultado": "pendiente",
        "goles_local_final": "",
        "goles_visita_final": "",
        "minuto_resolucion": "",
        "fecha_hora_resolucion": "",
    }
    try:
        _append_historial(fila)
    except Exception as e:
        print(f"[{datetime.now()}] Aviso: no se pudo registrar la alerta en {ARCHIVO_HISTORIAL} ({e}).")


def actualizar_resultados_pendientes(partidos_actuales):
    """
    Revisa las alertas con resultado 'pendiente' contra los partidos en
    vivo de ESTE mismo ciclo (no gasta solicitudes extra a SofaScore) y
    las resuelve:

    - 'fallo': el marcador combinado subio desde la alerta -> entro al
      menos un gol mas, la señal fue incorrecta para el mercado "no mas
      goles". Se resuelve apenas se detecta, sin esperar a nada mas.
    - 'acierto': el partido ya no aparece en la lista de partidos en
      vivo (se asume terminado) y pasaron al menos
      MINUTOS_ESPERA_CONFIRMAR_ACIERTO minutos de reloj real desde la
      alerta sin haber detectado un gol de mas mientras se lo pudo
      seguir viendo. Ver la limitacion documentada en
      MINUTOS_ESPERA_CONFIRMAR_ACIERTO: no se confirma con una consulta
      aparte al terminar, para no gastar trafico extra.

    Devuelve un resumen {"pendientes", "aciertos", "fallos"} (contando
    TODO el historial, no solo lo resuelto en este ciclo) para el log.
    """
    filas = _leer_historial()
    if not filas:
        return {"pendientes": 0, "aciertos": 0, "fallos": 0}

    partidos_por_id = {str(p["event_id"]): p for p in partidos_actuales}
    ahora = time.time()
    cambios = False

    for fila in filas:
        if fila["resultado"] != "pendiente":
            continue

        actual = partidos_por_id.get(fila["match_id"])
        if actual is not None:
            goles_actual = (actual["goles_local"] or 0) + (actual["goles_visita"] or 0)
            try:
                goles_alerta = int(fila["goles_local_alerta"]) + int(fila["goles_visita_alerta"])
            except (TypeError, ValueError):
                continue
            if goles_actual > goles_alerta:
                fila["resultado"] = "fallo"
                fila["goles_local_final"] = actual["goles_local"]
                fila["goles_visita_final"] = actual["goles_visita"]
                fila["minuto_resolucion"] = actual["minuto"] if actual["minuto"] is not None else ""
                fila["fecha_hora_resolucion"] = datetime.now().isoformat(timespec="seconds")
                cambios = True
        else:
            try:
                timestamp_alerta = float(fila.get("timestamp_alerta") or 0)
            except ValueError:
                timestamp_alerta = 0
            if timestamp_alerta and (ahora - timestamp_alerta) >= (MINUTOS_ESPERA_CONFIRMAR_ACIERTO * 60):
                fila["resultado"] = "acierto"
                fila["goles_local_final"] = fila["goles_local_alerta"]
                fila["goles_visita_final"] = fila["goles_visita_alerta"]
                fila["fecha_hora_resolucion"] = datetime.now().isoformat(timespec="seconds")
                cambios = True

    if cambios:
        try:
            _escribir_historial(filas)
        except Exception as e:
            print(f"[{datetime.now()}] Aviso: no se pudo actualizar {ARCHIVO_HISTORIAL} ({e}).")

    pendientes = sum(1 for f in filas if f["resultado"] == "pendiente")
    aciertos = sum(1 for f in filas if f["resultado"] == "acierto")
    fallos = sum(1 for f in filas if f["resultado"] == "fallo")
    return {"pendientes": pendientes, "aciertos": aciertos, "fallos": fallos}


def resumen_calibracion(archivo=ARCHIVO_HISTORIAL):
    """
    Herramienta de analisis MANUAL (no se usa en el loop del bot). Lee
    el historial de alertas ya resueltas y compara, tramo por tramo de
    probabilidad, lo que dijo el modelo contra el % de aciertos real.
    Es la unica forma seria de saber si conviene subir o bajar los
    umbrales, o si el modelo esta bien calibrado tal como esta.

    Uso tipico (en una consola aparte, despues de dejar correr el bot
    un tiempo):
        python -c "from nogolbot import resumen_calibracion as f; f()"
    """
    filas = _leer_historial(archivo)
    resueltas = [f for f in filas if f["resultado"] in ("acierto", "fallo")]
    if not resueltas:
        print(
            "Todavia no hay alertas resueltas en el historial (o el archivo no "
            "existe). Dejá correr el bot un tiempo y volvé a intentar."
        )
        return

    tramos = [(0.65, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 1.01)]
    print(f"\n{'Tramo de probabilidad':<24}{'N':>5}{'Aciertos':>10}{'% real':>10}{'Prob. media modelo':>21}")
    print("-" * 70)
    for lo, hi in tramos:
        en_tramo = [f for f in resueltas if lo <= float(f["prob_modelo"]) < hi]
        if not en_tramo:
            continue
        n = len(en_tramo)
        aciertos = sum(1 for f in en_tramo if f["resultado"] == "acierto")
        pct_real = aciertos / n * 100
        prob_media = sum(float(f["prob_modelo"]) for f in en_tramo) / n * 100
        etiqueta = f"{lo*100:.0f}-{hi*100:.0f}%"
        print(f"{etiqueta:<24}{n:>5}{aciertos:>10}{pct_real:>9.1f}%{prob_media:>20.1f}%")
    print("-" * 70)

    for etiqueta, filtro in (
        ("Con 0 señales extra (baja confianza)", lambda f: int(f["n_señales_extra"]) == 0),
        ("Con 1+ señales extra", lambda f: int(f["n_señales_extra"]) >= 1),
    ):
        subset = [f for f in resueltas if filtro(f)]
        if not subset:
            continue
        n = len(subset)
        aciertos = sum(1 for f in subset if f["resultado"] == "acierto")
        print(f"{etiqueta}: {aciertos}/{n} aciertos ({aciertos/n*100:.1f}%)")

    print(
        f"\nSi el '% real' de un tramo queda muy por debajo de la 'Prob. media "
        f"modelo' de ese mismo tramo, el modelo esta siendo optimista ahi (subi "
        f"el umbral o revisa los factores). Si queda muy por encima, el modelo "
        f"esta siendo conservador (hay margen para bajar el umbral con cautela).\n"
        f"Total de alertas resueltas: {len(resueltas)}. Con menos de ~30-50 por "
        f"tramo, tratá estos porcentajes como orientativos, no definitivos."
    )


def _minuto_desde_evento(status, time_obj):
    """
    Calcula el minuto de juego a partir de los campos crudos que
    devuelve SofaScore (`status` y `time` del evento).

    SofaScore no entrega un "minuto actual" directo para partidos en
    vivo: hay que derivarlo de `time.currentPeriodStartTimestamp` (el
    epoch en el que arrancó el tiempo/periodo actual) más 45' de base
    si ya vamos en el segundo tiempo. Esto es un detalle interno de su
    API no oficial: si SofaScore cambia este formato, esta función es
    el único lugar que hay que ajustar.

    Devuelve None si el partido no está en curso (no empezó, terminó,
    suspendido, etc.) o si no se pudo determinar el minuto.
    """
    tipo = (status or {}).get("type")
    descripcion = ((status or {}).get("description") or "").lower()

    if tipo in ("notstarted", "postponed", "canceled", "cancelled", "suspended", "abandoned"):
        return None
    if tipo == "finished":
        return 90
    if tipo == "halftime" or "halftime" in descripcion or "half-time" in descripcion or "descanso" in descripcion:
        return 45

    # Tiempo extra (prorroga): la logica de abajo solo sabe sumar la
    # base de 45' del 2do tiempo regular. En prorroga, esa base no
    # aplica (serian 90' o 105' segun el periodo) y no tenemos forma
    # confiable de distinguir "1er tiempo extra" de "2do tiempo extra"
    # solo con esta descripcion. Preferimos NO vigilar el partido a
    # vigilarlo con un minuto mal calculado, que en este mercado puede
    # costar dinero real. Si tu liga usa prorroga seguido y queres
    # cubrirla, confirma primero el campo real con
    # imprimir_estadisticas_crudas()/inspeccionando el evento crudo.
    if (
        "extra time" in descripcion or "et " in descripcion or descripcion.startswith("et")
        or "tiempo extra" in descripcion or "prorroga" in descripcion or "prórroga" in descripcion
    ):
        return None

    if tipo == "inprogress":
        inicio_periodo = (time_obj or {}).get("currentPeriodStartTimestamp")
        if not inicio_periodo:
            return None
        ahora = time.time()
        transcurrido_periodo = (ahora - inicio_periodo) / 60
        # Si la descripción menciona el 2do tiempo, sumamos la base de
        # 45' del primer tiempo. A diferencia de la version anterior,
        # NO topamos el resultado a 90': el descuento se maneja en
        # probabilidad_sin_mas_goles (ver DESCUENTO_2T_ASUMIDO_DEFAULT),
        # no aca, para no perder informacion del minuto real.
        base = 45 if ("2nd" in descripcion or "second half" in descripcion or "segunda" in descripcion) else 0
        minuto = max(0, base + transcurrido_periodo)
        if minuto > MINUTO_MAXIMO_CONFIABLE:
            # Ver comentario de MINUTO_MAXIMO_CONFIABLE: esto casi
            # siempre es un dato viejo/atascado, no un partido real
            # que sigue en curso a esta altura.
            return None
        return minuto

    return None


def _periodos_relevantes(periodos):
    """Prefiere el periodo 'ALL' (partido completo); si no viene, usa todos."""
    if not periodos:
        return []
    periodo_completo = next((p for p in periodos if p.get("period") == "ALL"), None)
    return [periodo_completo] if periodo_completo else periodos


def _items_planos_de_estadisticas(periodos):
    """Aplana groups -> statisticsItems de todos los periodos relevantes en una sola lista."""
    items = []
    for periodo in _periodos_relevantes(periodos):
        if not periodo:
            continue
        for grupo in periodo.get("groups", []):
            items.extend(grupo.get("statisticsItems", []))
    return items


def _valor_total_item(item):
    """Suma home+away de un item de estadística, con fallback a texto."""
    home_val = item.get("homeValue")
    away_val = item.get("awayValue")
    if home_val is None or away_val is None:
        try:
            home_val = float(str(item.get("home", "0")).strip() or 0)
            away_val = float(str(item.get("away", "0")).strip() or 0)
        except ValueError:
            return None
    if home_val is None or away_val is None:
        return None
    return (home_val or 0) + (away_val or 0)


def _buscar_metrica(items, claves, fragmentos_nombre, excluir_fragmentos=()):
    """
    Busca items de estadística por su `key` exacta (mas confiable) o,
    si no matchea ninguna clave conocida, por fragmentos en el `name`
    (mas frágil: SofaScore puede fraseario distinto). `excluir_fragmentos`
    sirve para no confundir, ej. "Big chance missed" con "Big chance created".

    IMPORTANTE: se SUMAN todos los items que matcheen, no se devuelve
    solo el primero. Cuando SofaScore trae el periodo agregado "ALL"
    (lo normal en ligas top), va a haber un solo item y esto no cambia
    nada. Pero si "ALL" no viene y el partido queda representado por
    1er y 2do tiempo por separado (ver _periodos_relevantes), quedarse
    con el primer match perdería en silencio la estadística de la
    otra mitad del partido — justo la parte más reciente y relevante
    para este modelo.
    """
    total = None
    for item in items:
        clave = (item.get("key") or "").lower()
        nombre = (item.get("name") or "").lower()
        if any(excl in nombre for excl in excluir_fragmentos):
            continue
        if clave in claves or any(frag in nombre for frag in fragmentos_nombre):
            valor = _valor_total_item(item)
            if valor is not None:
                total = (total or 0) + valor
    return total


# Mapeos de claves/nombres conocidos de SofaScore para cada señal. Son
# los nombres mas comunes documentados por la comunidad que usa su API
# no oficial, pero SofaScore puede variarlos por deporte/liga/idioma.
# Si una señal te da siempre None en un partido donde la web SÍ muestra
# ese dato, corré imprimir_estadisticas_crudas(event_id) y agregá la
# clave/nombre real que encuentres a estos sets/tuplas.
_CLAVES_TIROS_A_PUERTA = {"shotsongoal", "shotsontarget"}
_NOMBRES_TIROS_A_PUERTA = ("on target", "on goal")

_CLAVES_TIROS_TOTALES = {"totalshotsongoal", "totalshots", "shotstotal"}
_NOMBRES_TIROS_TOTALES = ("total shots",)

_CLAVES_GRANDES_OCASIONES = {"bigchancecreated"}
_NOMBRES_GRANDES_OCASIONES = ("big chance",)
_EXCLUIR_GRANDES_OCASIONES = ("missed",)  # evita sumar "Big chances missed" aparte

_CLAVES_XG = {"expectedgoals", "xg"}
_NOMBRES_XG = ("expected goals",)

_CLAVES_TARJETAS_ROJAS = {"redcards", "redcard"}
_NOMBRES_TARJETAS_ROJAS = ("red card",)

# Señal opcional (ver comentario junto a FRACCION_KELLY/USAR_MOMENTUM en
# CONFIG): se extrae siempre, pero sin peso en PESOS_SEÑALES por
# default. Confirmá la clave real con imprimir_estadisticas_crudas()
# antes de activarla - "cornerkicks" es la mas comun documentada por la
# comunidad, pero SofaScore puede variarla.
_CLAVES_CORNERS = {"cornerkicks", "corners"}
_NOMBRES_CORNERS = ("corner",)


def obtener_estadisticas_partido(event_id):
    """
    Consulta las estadísticas del partido en SofaScore UNA sola vez y
    extrae de ahí todas las señales que usa el modelo: tiros a puerta,
    tiros totales, grandes ocasiones y xG. Cualquier señal que no se
    encuentre en la respuesta queda como None (el modelo la ignora y
    renormaliza los pesos entre las que sí están).

    Se llama como máximo una vez por ciclo para cada partido que está
    en la ventana de vigilancia y que todavía no disparó una alerta
    (ver `partidos_ya_avisados` en analizar_ciclo). No se cachea entre
    ciclos: estos valores cambian minuto a minuto, y un valor viejo
    reutilizado sesgaría el modelo.
    """
    global _contador_sin_stats_404_sesion

    # Se normaliza a str siempre (ver comentario en partidos_ya_avisados):
    # tras cargar el estado desde ARCHIVO_ESTADO, las claves vuelven como
    # string por el round-trip a JSON, así que hay que insertar/consultar
    # siempre con el mismo tipo para que el cache funcione entre ciclos
    # y entre reinicios.
    clave = str(event_id)

    if clave in _partidos_sin_stats_conocidos:
        return {}

    _registrar_solicitud()
    try:
        periodos = _client.get_event_statistics(event_id)
    except SofaScoreError as e:
        if e.status == 404:
            # SofaScore simplemente no tiene estadisticas detalladas
            # publicadas para este partido (normal en ligas amateur o
            # de cobertura floja) - no es un error real del bot, asi
            # que no lo tratamos como tal en la consola: se cuenta
            # nomas, se recuerda para no volver a pedirlo, y se resume
            # al final del ciclo. El modelo sigue funcionando igual,
            # solo con la señal de goles.
            _contador_sin_stats_404_sesion += 1
            _partidos_sin_stats_conocidos[clave] = time.time()
            return {}
        print(f"[{datetime.now()}] Error consultando estadísticas del partido {event_id}: {e}")
        return {}
    except Exception as e:
        print(f"[{datetime.now()}] Error inesperado consultando estadísticas del partido {event_id}: {e}")
        return {}

    items = _items_planos_de_estadisticas(periodos)
    if not items:
        return {}

    return {
        "tiros_a_puerta": _buscar_metrica(items, _CLAVES_TIROS_A_PUERTA, _NOMBRES_TIROS_A_PUERTA),
        "tiros_totales": _buscar_metrica(items, _CLAVES_TIROS_TOTALES, _NOMBRES_TIROS_TOTALES),
        "grandes_ocasiones": _buscar_metrica(
            items, _CLAVES_GRANDES_OCASIONES, _NOMBRES_GRANDES_OCASIONES,
            excluir_fragmentos=_EXCLUIR_GRANDES_OCASIONES,
        ),
        "xg": _buscar_metrica(items, _CLAVES_XG, _NOMBRES_XG),
        # No es una señal "de ritmo" como las demás (no se compara
        # contra un promedio esperado): es un conteo que se usa en
        # probabilidad_sin_mas_goles como un multiplicador aparte.
        "tarjetas_rojas": _buscar_metrica(items, _CLAVES_TARJETAS_ROJAS, _NOMBRES_TARJETAS_ROJAS),
        # Opcional, sin peso por default (ver _CLAVES_CORNERS arriba).
        "corners": _buscar_metrica(items, _CLAVES_CORNERS, _NOMBRES_CORNERS),
    }


def obtener_intensidad_momentum(event_id):
    """
    EXPERIMENTAL. Usa el gráfico de "momentum de ataque" de SofaScore
    (client.get_event_graph) como aproximación a "ataques peligrosos":
    es la señal que muestra qué equipo está presionando en cada minuto.

    No la uso por default (ver USAR_MOMENTUM) porque no pude confirmar
    en vivo el rango real de sus valores. Devuelve el promedio de la
    magnitud absoluta de los últimos puntos del gráfico (sin importar
    a qué equipo favorecen, solo "cuánta presión hay ahora mismo"), o
    None si no se pudo obtener.
    """
    _registrar_solicitud()
    try:
        grafico = _client.get_event_graph(event_id)
    except Exception as e:
        print(f"[{datetime.now()}] Error consultando momentum del partido {event_id}: {e}")
        return None

    puntos = (grafico or {}).get("graphPoints") or []
    if not puntos:
        return None

    ultimos = puntos[-5:]  # últimos ~5 minutos de datos
    valores = [abs(p.get("value", 0)) for p in ultimos if isinstance(p.get("value"), (int, float))]
    if not valores:
        return None
    return sum(valores) / len(valores)


def imprimir_estadisticas_crudas(event_id):
    """
    Herramienta de diagnóstico manual (no se usa en el loop del bot).
    Imprime TODOS los grupos/items que SofaScore devuelve para un
    partido, con su key, name y valores home/away tal cual vienen.

    Útil para: agarrar el `event_id` de un partido en vivo (se muestra
    en el detalle de cada ciclo) y correr esto en una consola aparte
    para confirmar los nombres/keys reales antes de confiar en los
    mapeos de _CLAVES_*/_NOMBRES_* de arriba, por ejemplo:

        python -c "from nogolbot import imprimir_estadisticas_crudas as f; f(123456)"
    """
    periodos = _client.get_event_statistics(event_id)
    for periodo in periodos:
        print(f"\n--- Periodo: {periodo.get('period')} ---")
        for grupo in periodo.get("groups", []):
            print(f"  [{grupo.get('groupName')}]")
            for item in grupo.get("statisticsItems", []):
                print(f"    key={item.get('key')!r} name={item.get('name')!r} "
                      f"home={item.get('homeValue', item.get('home'))} "
                      f"away={item.get('awayValue', item.get('away'))}")


# ============== MODELO ==============

def _factor_ritmo(valor_real, promedio_90, minuto_actual, valor_minimo):
    """
    Compara un valor real acumulado contra lo "esperado" para este
    minuto (promedio_90 prorrateado), y devuelve un factor recortado
    a [0.3, 2.5] — igual lógica que ya usaba el modelo para goles y
    tiros a puerta, generalizada para cualquier métrica.
    """
    esperado = max(promedio_90 * (minuto_actual / 90), valor_minimo)
    factor = valor_real / esperado
    return max(0.3, min(factor, 2.5))


def probabilidad_sin_mas_goles(minuto_actual, goles_ya_marcados, estadisticas=None,
                                promedio_liga=PROMEDIO_GOLES_LIGA_DEFAULT,
                                descuento_2t_asumido=DESCUENTO_2T_ASUMIDO_DEFAULT):
    """
    Estima P(no entra ningun gol mas) usando un modelo de Poisson.

    lambda_restante = promedio_liga * (minutos_restantes/90) * factor_final
                      * factor_tramo_final * factor_tarjeta_roja
    P(0 goles en lo que resta) = e^(-lambda_restante)

    factor_final es un PROMEDIO PONDERADO (según PESOS_SEÑALES) de todas
    las señales de ritmo disponibles para este partido:
    - goles: goles reales vs. esperados para esta liga a este minuto
      (siempre disponible)
    - tiros_a_puerta, tiros_totales, grandes_ocasiones, xg: cada uno
      real vs. esperado a este minuto, tomados de `estadisticas` (dict
      devuelto por obtener_estadisticas_partido; cualquier clave puede
      venir en None si SofaScore no la trajo para este partido)

    Los pesos se RENORMALIZAN sobre las señales realmente disponibles,
    así que si un partido solo trae goles (sin stats), el resultado es
    igual a usar solo esa señal — no se rompe ni se sesga por datos
    faltantes.

    Dos correcciones importantes sobre la version anterior:

    1) Descuento: en vez de tratar minuto>90 como "partido terminado"
       (lambda=0, prob=1.0 instantaneo), se asume que el partido dura
       90 + `descuento_2t_asumido` minutos. Esto evita la falsa certeza
       justo quando el descuento -uno de los tramos de mayor riesgo de
       gol de todo el partido- recien empieza. minutos_restantes nunca
       llega literalmente a 0 mientras el partido siga "inprogress":
       se usa un piso de medio minuto para reflejar que, sin
       confirmacion de pitido final, siempre queda algo de incertidumbre.

    2) Intensidad del tramo final: los goles no se reparten parejo en
       los 90'. Multiples estudios muestran que el tramo 75-90'+
       concentra ~20-26% de los goles totales (vs. 16.7% si la tasa
       fuese constante), ver FACTOR_INTENSIDAD_TRAMO_FINAL. Sin esto,
       el modelo subestima el riesgo justo en el tramo que vigila.

    Ademas, si `estadisticas` trae tarjetas_rojas > 0, se aplica
    FACTOR_TARJETA_ROJA: hay evidencia academica de que el goleo total
    tiende a subir tras una expulsion (el equipo con superioridad
    numerica sube su tasa de gol mas de lo que baja el que queda con
    diez).
    """
    duracion_asumida = 90 + descuento_2t_asumido
    minutos_restantes = max(duracion_asumida - minuto_actual, 0.5)

    estadisticas = estadisticas or {}

    # El de goles se calcula igual que antes (promedio_liga ya representa
    # el "promedio por 90 min", así que se lo pasamos directo a _factor_ritmo).
    factor_goles = _factor_ritmo(goles_ya_marcados, promedio_liga, minuto_actual, 0.15)

    factores_disponibles = {"goles": factor_goles}

    tiros_a_puerta = estadisticas.get("tiros_a_puerta")
    if tiros_a_puerta is not None:
        factores_disponibles["tiros_a_puerta"] = _factor_ritmo(
            tiros_a_puerta, PROMEDIO_TIROS_A_PUERTA_90, minuto_actual, 0.5)

    tiros_totales = estadisticas.get("tiros_totales")
    if tiros_totales is not None:
        factores_disponibles["tiros_totales"] = _factor_ritmo(
            tiros_totales, PROMEDIO_TIROS_TOTALES_90, minuto_actual, 1.0)

    grandes_ocasiones = estadisticas.get("grandes_ocasiones")
    if grandes_ocasiones is not None:
        factores_disponibles["grandes_ocasiones"] = _factor_ritmo(
            grandes_ocasiones, PROMEDIO_GRANDES_OCASIONES_90, minuto_actual, 0.2)

    xg = estadisticas.get("xg")
    if xg is not None:
        factores_disponibles["xg"] = _factor_ritmo(xg, promedio_liga, minuto_actual, 0.1)

    corners = estadisticas.get("corners")
    if corners is not None and "corners" in PESOS_SEÑALES:
        # Igual patron opt-in que momentum: se calcula siempre, pero solo
        # pesa si vos agregaste "corners" a PESOS_SEÑALES a proposito.
        factores_disponibles["corners"] = _factor_ritmo(
            corners, PROMEDIO_CORNERS_90, minuto_actual, 0.5)

    momentum = estadisticas.get("momentum")
    if momentum is not None and "momentum" in PESOS_SEÑALES:
        # Sin baseline "esperado" claro (ver aviso en USAR_MOMENTUM):
        # se usa como factor directo ya normalizado por quien lo activó.
        factores_disponibles["momentum"] = max(0.3, min(momentum, 2.5))

    peso_total = sum(PESOS_SEÑALES.get(clave, 0) for clave in factores_disponibles)
    if peso_total <= 0:
        factor_final = factor_goles  # fallback de seguridad
    else:
        factor_final = sum(
            PESOS_SEÑALES.get(clave, 0) * valor for clave, valor in factores_disponibles.items()
        ) / peso_total

    # Tramo final: inflamos el lambda base (no factor_final, que ya
    # refleja el ritmo propio de ESTE partido) porque lo que corregimos
    # es el supuesto de tasa constante en el tiempo, no el ritmo del
    # partido puntual.
    factor_tramo_final = (
        FACTOR_INTENSIDAD_TRAMO_FINAL if minuto_actual >= MINUTO_INICIO_TRAMO_FINAL else 1.0
    )

    tarjetas_rojas = estadisticas.get("tarjetas_rojas")
    factor_tarjeta_roja = FACTOR_TARJETA_ROJA if (tarjetas_rojas or 0) > 0 else 1.0

    lambda_restante = (
        promedio_liga * (minutos_restantes / 90) * factor_final
        * factor_tramo_final * factor_tarjeta_roja
    )
    return math.exp(-lambda_restante)


def valor_esperado_apuesta(prob_modelo, cuota_decimal, banca=None):
    """
    Compara la probabilidad del modelo contra la cuota REAL que ofrece
    tu casa de apuestas para "no mas goles", y devuelve un diccionario
    con la probabilidad implicita de esa cuota, el "edge" (diferencia),
    el valor esperado por unidad apostada, y una SUGERENCIA de tamaño
    de apuesta (Kelly fraccionado, ver FRACCION_KELLY/TOPE_FRACCION_
    BANCA_POR_APUESTA en CONFIG).

    Un umbral fijo de probabilidad (UMBRAL_PROBABILIDAD) no alcanza
    para saber si una apuesta tiene valor: las casas de apuestas cargan
    un margen (overround/vig) en sus cuotas, asi que una probabilidad
    de modelo del 65% puede seguir siendo una MALA apuesta si la cuota
    implica, por ejemplo, 78%.

    Sobre el Kelly sugerido: se usa una FRACCION conservadora del Kelly
    completo (no el completo) porque Kelly completo asume que tu
    probabilidad estimada es exacta, y la nuestra es una aproximacion
    heuristica todavia sin calibrar con datos reales (ver
    resumen_calibracion). Ademas se aplica un tope duro que nunca se
    cruza, pase lo que pase. Si no hay edge real (ev <= 0), la
    sugerencia siempre es 0 - Kelly nunca recomienda apostar sin edge.

    Uso tipico (en una consola aparte, con la cuota que ves en tu casa
    de apuestas en el momento de la señal):

        from nogolbot import valor_esperado_apuesta
        valor_esperado_apuesta(0.72, 1.25, banca=200)

    prob_modelo: probabilidad estimada por este bot (0-1).
    cuota_decimal: cuota decimal de tu casa de apuestas (ej. 1.25 = 25%
        de ganancia sobre lo apostado si acertas).
    banca: opcional. Si la das, se agrega "monto_sugerido" (en las
        mismas unidades que la banca) ademas de la fraccion.
    """
    if cuota_decimal <= 1:
        raise ValueError("La cuota decimal debe ser mayor a 1.")
    if not 0 <= prob_modelo <= 1:
        raise ValueError("prob_modelo debe estar entre 0 y 1.")

    prob_implicita = 1 / cuota_decimal
    b = cuota_decimal - 1  # ganancia neta por unidad apostada si acertas
    ev_por_unidad = prob_modelo * b - (1 - prob_modelo)

    # Kelly completo para una apuesta binaria: f* = (b*p - q) / b.
    # Se recorta a 0 si da negativo (sin edge, Kelly no sugiere apostar).
    kelly_completo = max(0.0, (b * prob_modelo - (1 - prob_modelo)) / b)
    fraccion_sugerida = min(kelly_completo * FRACCION_KELLY, TOPE_FRACCION_BANCA_POR_APUESTA)

    resultado = {
        "prob_modelo": prob_modelo,
        "cuota_decimal": cuota_decimal,
        "prob_implicita_cuota": prob_implicita,
        "edge": prob_modelo - prob_implicita,
        "valor_esperado_por_unidad": ev_por_unidad,
        "tiene_valor": ev_por_unidad > 0,
        "kelly_completo": kelly_completo,
        "fraccion_banca_sugerida": fraccion_sugerida,
    }
    if banca is not None:
        resultado["monto_sugerido"] = round(banca * fraccion_sugerida, 2)
    return resultado


# ============== VEREDICTO CON IA (opcional) ==============

def generar_veredicto_ia(contexto):
    if MODO_IA == "ninguno":
        return None
    if MODO_IA == "api" and not GEMINI_API_KEY:
        # Ya se avisó una vez al arrancar (ver _validar_configuracion);
        # evitamos golpear la API de Gemini con una key vacía en cada
        # alerta (fallaría siempre, y solo ensuciaría la consola).
        return None

    prompt = (
        "Eres un analista de apuestas deportivas experto en futbol. "
        f"Partido: {contexto['local']} {contexto['goles_local']}-{contexto['goles_visita']} "
        f"{contexto['visita']}\n"
        f"Competicion: {contexto['competicion']}\n"
        f"Minuto: {contexto['minuto']}'\n"
        f"Probabilidad estadistica calculada de que NO entre ningun gol mas: "
        f"{contexto['prob']*100:.1f}%\n\n"
        "En maximo 3 frases, da tu veredicto sobre si esta probabilidad "
        "parece razonable dado el contexto (marcador, minuto, tipo de "
        "competicion), y menciona algun factor cualitativo que el modelo "
        "estadistico no puede ver. No inventes datos concretos (lesiones, "
        "alineaciones, clima) que no te he dado."
    )

    try:
        if MODO_IA == "local":
            r = requests.post(
                OLLAMA_URL,
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                timeout=60,
            )
            r.raise_for_status()
            return r.json().get("response", "").strip()

        elif MODO_IA == "api":
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
            )
            body = {"contents": [{"parts": [{"text": prompt}]}]}
            r = requests.post(url, json=body, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()

    except Exception as e:
        print(f"[{datetime.now()}] Error generando veredicto IA ({MODO_IA}): {e}")
        return None


# ============== DATOS EN VIVO (cualquier liga del mundo) ==============

def obtener_partidos_en_vivo():
    """
    Trae TODOS los partidos en vivo de TODAS las ligas del mundo en una
    sola solicitud, usando SofaScore (via pysofascore), y los normaliza
    a un formato simple que usa el resto del bot.

    Devuelve None (no []) si la solicitud falló - la distinción importa:
    [] significa legítimamente "ahora mismo no hay partidos en vivo",
    mientras que None significa "no pudimos preguntar" y NO debe
    tratarse como si no hubiera partidos (ver analizar_ciclo/main, que
    usan esto para activar el backoff en vez de seguir como si nada).
    """
    global _errores_consecutivos_sofascore

    _registrar_solicitud()
    try:
        eventos = _client.get_live_events("football")
    except SofaScoreError as e:
        _errores_consecutivos_sofascore += 1
        print(f"[{datetime.now()}] Error consultando SofaScore (fallo consecutivo #{_errores_consecutivos_sofascore}): {e}")
        return None
    except Exception as e:
        _errores_consecutivos_sofascore += 1
        print(f"[{datetime.now()}] Error inesperado consultando SofaScore (fallo consecutivo #{_errores_consecutivos_sofascore}): {e}")
        return None

    _errores_consecutivos_sofascore = 0
    partidos = []
    for ev in eventos:
        try:
            home = (ev.get("homeTeam") or {}).get("name", "Local")
            away = (ev.get("awayTeam") or {}).get("name", "Visitante")
            goles_local = (ev.get("homeScore") or {}).get("current") or 0
            goles_visita = (ev.get("awayScore") or {}).get("current") or 0

            torneo = ev.get("tournament") or {}
            nombre_torneo = torneo.get("name", "Liga desconocida")
            categoria = (torneo.get("category") or {}).get("name", "")
            competicion = f"{nombre_torneo} ({categoria})" if categoria else nombre_torneo

            minuto = _minuto_desde_evento(ev.get("status"), ev.get("time"))

            partidos.append({
                "event_id": ev.get("id"),
                "local": home,
                "visita": away,
                "goles_local": goles_local,
                "goles_visita": goles_visita,
                "competicion": competicion,
                "minuto": int(minuto) if minuto is not None else None,
            })
        except Exception as e:
            print(f"[{datetime.now()}] Error procesando un evento de SofaScore, se omite: {e}")
            continue

    return partidos


def _señales_extra_ponderadas():
    """
    Cualquier clave de PESOS_SEÑALES excepto "goles" cuenta como señal
    "extra" de ritmo. A proposito NO es una tupla fija a mano: si vos
    agregás "momentum" o "corners" (o cualquier otra) a PESOS_SEÑALES,
    se refleja aca automaticamente sin tener que tocar dos lugares
    distintos del codigo y que se desincronicen entre si (eso pasaba
    antes: activar USAR_MOMENTUM no sumaba a este conteo aunque sí
    pesara en el modelo).

    OJO: tarjetas_rojas NO puede estar en PESOS_SEÑALES a proposito -
    no entra al promedio ponderado, actua aparte como multiplicador
    (ver FACTOR_TARJETA_ROJA en probabilidad_sin_mas_goles), asi que un
    partido que solo trae "tarjetas rojas: 0" en el log tiene, en la
    practica, CERO señales de ritmo extra: el modelo para ese partido
    es, matematicamente, identico a un modelo que solo mira el
    marcador y el minuto.
    """
    return tuple(clave for clave in PESOS_SEÑALES if clave != "goles")


def _resumen_confianza_señales(estadisticas):
    """
    Cuenta cuantas de las señales de ritmo ponderadas (ver
    _señales_extra_ponderadas) trajo SofaScore para este partido, y que
    fraccion del peso TOTAL del modelo terminan representando.

    Por que importa: cuando esto da 0/N, la probabilidad de esa alerta
    sale de exactamente los mismos dos datos que ya tiene delante
    cualquier casa de apuestas (marcador y minuto) - es la version mas
    simple y mas facil de que YA este reflejada en la cuota, por lo
    tanto la que menos "edge" real deberia asumirse que tiene. Cuantas
    mas señales independientes disponibles, mas se aleja el modelo de
    ser solo una relectura del marcador.
    """
    estadisticas = estadisticas or {}
    señales_extra = _señales_extra_ponderadas()
    disponibles = [c for c in señales_extra if estadisticas.get(c) is not None]
    peso_usado = PESOS_SEÑALES.get("goles", 0) + sum(PESOS_SEÑALES.get(c, 0) for c in disponibles)
    peso_total_posible = PESOS_SEÑALES.get("goles", 0) + sum(
        PESOS_SEÑALES.get(c, 0) for c in señales_extra
    )
    return {
        "n_extra": len(disponibles),
        "n_extra_posibles": len(señales_extra),
        "fraccion_peso_usado": (peso_usado / peso_total_posible) if peso_total_posible else 0.0,
    }


def _formatear_estadisticas(stats):
    """Texto corto con las señales disponibles, para logs y alertas."""
    etiquetas = {
        "tiros_a_puerta": "tiros a puerta",
        "tiros_totales": "tiros totales",
        "grandes_ocasiones": "grandes ocasiones",
        "xg": "xG",
        "momentum": "momentum",
        "tarjetas_rojas": "tarjetas rojas",
        "corners": "corners",
    }
    partes = []
    for clave, etiqueta in etiquetas.items():
        valor = stats.get(clave)
        if valor is None:
            continue
        partes.append(f"{etiqueta}: {valor:.2f}" if clave in ("xg", "momentum") else f"{etiqueta}: {valor:g}")
    return ", ".join(partes) if partes else "sin estadísticas extra disponibles"


# ============== LOOP PRINCIPAL ==============

def analizar_ciclo():
    """
    Analiza un ciclo de partidos en vivo. Devuelve True si hay al menos
    un partido entre MINUTO_ALERTA_TEMPRANA y MINUTO_FIN_VIGILANCIA
    (para que main() decida si el próximo chequeo debe ser rápido o
    lento).

    Solo se imprime linea por linea el partido que de verdad importa
    para el mercado que vigilamos (cerca o dentro de la ventana
    MINUTO_INICIO_VIGILANCIA-MINUTO_FIN_VIGILANCIA). El resto -partidos
    recien arrancando, ya pasados de esa ventana, sin minuto reportado,
    o ya avisados- se resume en un solo renglon al final, salvo que
    actives MOSTRAR_TODOS_LOS_PARTIDOS_LEJANOS para debug.
    """
    partidos = obtener_partidos_en_vivo()
    if partidos is None:
        # Fallo de red/API (ver obtener_partidos_en_vivo): no hay nada
        # que analizar este ciclo. main() se encarga del backoff via
        # _errores_consecutivos_sofascore.
        return False

    _purgar_cache_vencido()
    resumen_hist = actualizar_resultados_pendientes(partidos)

    hay_partido_cerca_del_final = False
    contador_sin_minuto = 0
    contador_lejos_de_vigilancia = 0
    contador_acercandose = 0
    contador_ya_avisados = 0
    contador_baja_confianza_no_alertada = 0
    contador_fuera_ventana_cuotas = 0

    for p in partidos:
        match_id = p["event_id"]
        local = p["local"]
        visita = p["visita"]
        goles_local = p["goles_local"]
        goles_visita = p["goles_visita"]
        goles_totales = goles_local + goles_visita
        competicion = p["competicion"]
        minuto = p["minuto"]

        if minuto is None:
            # Sin minuto no hay nada que analizar (entretiempo sin
            # confirmar, dato roto, partido recien detectado, etc.):
            # no se imprime uno por uno, se cuenta nomas.
            contador_sin_minuto += 1
            if MOSTRAR_TODOS_LOS_PARTIDOS_LEJANOS:
                print(f"  - [{competicion}] {local} {goles_local}-{goles_visita} {visita} "
                      f"(id {match_id}, sin minuto reportado, ej. entretiempo o aún no confirmado)")
            continue

        if MINUTO_ALERTA_TEMPRANA <= minuto <= MINUTO_FIN_VIGILANCIA:
            hay_partido_cerca_del_final = True

        if minuto < MINUTO_ALERTA_TEMPRANA:
            # Todavia falta bastante (a veces mas de una hora): imprimir
            # esto cada ciclo, para los ~cientos de partidos en vivo en
            # cualquier momento dado, es puro ruido. Se cuenta nomas.
            contador_lejos_de_vigilancia += 1
            if MOSTRAR_TODOS_LOS_PARTIDOS_LEJANOS:
                print(f"  - [{competicion}] {local} {goles_local}-{goles_visita} {visita} "
                      f"(id {match_id}, min {minuto}' - lejos de la ventana de vigilancia)")
            continue

        if minuto < MINUTO_INICIO_VIGILANCIA:
            # Zona intermedia: ya paso MINUTO_ALERTA_TEMPRANA (65', por
            # eso ya activamos el polling rapido) pero todavia no llega
            # a MINUTO_INICIO_VIGILANCIA (75'), asi que no pedimos stats
            # ni alertamos - no aporta nada todavia. ANTES esta rama
            # tenia un "if MOSTRAR_DETALLE: None" que no hacia nada (era
            # codigo muerto) y el partido desaparecia sin contarse en
            # ningun lado del resumen, asi que el total de arriba nunca
            # cerraba con la suma de categorias. Ahora se cuenta aca,
            # igual que las demas categorias omitidas.
            contador_acercandose += 1
            continue

        if minuto > MINUTO_FIN_VIGILANCIA:
            # Ya pasamos la ventana en la que la cuota de "no mas goles"
            # todavia vale la pena (ver MINUTO_FIN_VIGILANCIA): no tiene
            # sentido gastar una consulta a SofaScore ni alertar sobre
            # esto. Se cuenta nomas, igual que las otras categorias
            # "omitidas".
            contador_fuera_ventana_cuotas += 1
            continue

        clave = f"{match_id}"

        if clave in partidos_ya_avisados:
            contador_ya_avisados += 1
            continue

        stats = obtener_estadisticas_partido(match_id)
        if USAR_MOMENTUM:
            stats["momentum"] = obtener_intensidad_momentum(match_id)
        prob = probabilidad_sin_mas_goles(minuto, goles_totales, estadisticas=stats)

        confianza = _resumen_confianza_señales(stats)
        umbral_efectivo = (
            UMBRAL_PROBABILIDAD_BAJA_CONFIANZA if confianza["n_extra"] == 0 else UMBRAL_PROBABILIDAD
        )

        prob_anterior, _ts_anterior = _ultima_prob_mostrada.get(clave, (None, None))
        debe_imprimir_detalle = MOSTRAR_DETALLE and (
            prob_anterior is None or abs(prob - prob_anterior) >= UMBRAL_CAMBIO_PROB_PARA_REIMPRIMIR
        )
        if debe_imprimir_detalle:
            print(f"  - [{competicion}] {local} {goles_local}-{goles_visita} {visita} "
                  f"(id {match_id}, min {minuto}', {_formatear_estadisticas(stats)}) "
                  f"-> probabilidad de 0 goles restantes: "
                  f"{prob*100:.1f}% (umbral: {umbral_efectivo*100:.0f}%, "
                  f"señales de ritmo: {confianza['n_extra']}/{confianza['n_extra_posibles']})")
            _ultima_prob_mostrada[clave] = (prob, time.time())

        if UMBRAL_PROBABILIDAD <= prob < umbral_efectivo:
            # Habria cruzado el umbral normal, pero es una señal 0/4
            # (solo marcador+minuto) que todavia no llega al umbral mas
            # exigente que le pedimos a esas señales. Se cuenta para que
            # sepas cuantas se estan quedando afuera por este filtro,
            # no se imprime linea por linea (ya la viste en el detalle).
            contador_baja_confianza_no_alertada += 1

        if prob >= umbral_efectivo:
            partidos_ya_avisados[clave] = time.time()

            contexto = {
                "local": local, "visita": visita,
                "goles_local": goles_local, "goles_visita": goles_visita,
                "competicion": competicion, "minuto": minuto, "prob": prob,
            }
            registrar_alerta(match_id, contexto, prob, umbral_efectivo, confianza)
            veredicto_ia = generar_veredicto_ia(contexto)

            mensaje = (
                f"\n{'='*60}\n"
                f"⚽ SEÑAL: posible 'no más goles'\n"
                f"{local} {goles_local}-{goles_visita} {visita}\n"
                f"Competición: {competicion}\n"
                f"Minuto: {minuto}'\n"
                f"Señales usadas: {_formatear_estadisticas(stats)} "
                f"(ritmo: {confianza['n_extra']}/{confianza['n_extra_posibles']} señales extra, "
                f"{confianza['fraccion_peso_usado']*100:.0f}% del peso del modelo)\n"
                f"Probabilidad estimada de 0 goles restantes: {prob*100:.1f}%\n"
            )
            if confianza["n_extra"] == 0:
                mensaje += (
                    "\n⚠️ Señal de BAJA confianza: SofaScore no trajo tiros a "
                    "puerta, tiros totales, grandes ocasiones ni xG para este "
                    "partido, asi que esta probabilidad sale SOLO del marcador "
                    "y el minuto - los mismos dos datos que ya tiene tu casa de "
                    "apuestas. Tratala con mas cautela que una señal respaldada "
                    "por varias métricas independientes.\n"
                )
            if veredicto_ia:
                mensaje += f"\n🤖 Veredicto IA: {veredicto_ia}\n"

            mensaje += (
                f"\nRecuerda: compara esta probabilidad contra la cuota que "
                f"te ofrece tu casa de apuestas para 'no más goles'. Un "
                f"umbral fijo de probabilidad no alcanza -las casas cargan "
                f"un margen en sus cuotas-, así que calculá el valor real "
                f"(y una sugerencia de tamaño de apuesta) con "
                f"valor_esperado_apuesta(prob, cuota, banca=tu_banca) antes "
                f"de decidir. Decisión final: tuya.\n"
                f"Esta alerta quedó guardada en {ARCHIVO_HISTORIAL} para "
                f"poder revisar más adelante, con resumen_calibracion(), si "
                f"el modelo está bien calibrado.\n"
                f"{'='*60}"
            )
            print(mensaje)

    print(f"[{datetime.now()}] Resumen del ciclo: {len(partidos)} partidos en vivo (todas las ligas)")
    print(f"  Omitidos → sin minuto: {contador_sin_minuto} · "
          f"lejos (<{MINUTO_ALERTA_TEMPRANA}'): {contador_lejos_de_vigilancia} · "
          f"acercándose ({MINUTO_ALERTA_TEMPRANA}-{MINUTO_INICIO_VIGILANCIA - 1}'): {contador_acercandose} · "
          f"fuera de ventana (>{MINUTO_FIN_VIGILANCIA}'): {contador_fuera_ventana_cuotas} · "
          f"ya avisados: {contador_ya_avisados} · "
          f"baja confianza sin alertar: {contador_baja_confianza_no_alertada}")
    print(f"  SofaScore → solicitudes en sesión: {_contador_solicitudes_sesion} · "
          f"sin stats (404) en sesión: {_contador_sin_stats_404_sesion}")
    print(f"  Historial → pendientes de resultado: {resumen_hist['pendientes']} · "
          f"aciertos: {resumen_hist['aciertos']} · fallos: {resumen_hist['fallos']}")

    _guardar_estado()
    return hay_partido_cerca_del_final


def _intervalo_con_backoff(intervalo_base):
    """Duplica el intervalo por cada fallo consecutivo, hasta BACKOFF_MAXIMO_SEGUNDOS."""
    if _errores_consecutivos_sofascore == 0:
        return intervalo_base
    backoff = intervalo_base * (2 ** min(_errores_consecutivos_sofascore - 1, 4))  # tope interno de x16
    return min(backoff, BACKOFF_MAXIMO_SEGUNDOS)


def main():
    _validar_configuracion()
    _cargar_estado()
    analizar_ciclo()
    _guardar_estado()
    _client.close()


if __name__ == "__main__":
    main()