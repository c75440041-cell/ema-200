# Bot BingX -> Telegram (solo cripto)

Escanea las monedas cripto USDT de BingX (futuros perpetuos) al cierre de cada vela.
Excluye acciones (NCSK*), indices (NCSI*), materias primas/petroleo (NCCO*), forex (NCFX*)
y tokens de acciones/oro (AAPLX, PAXG...).

## Modulos
1. **Toque EMA200**: avisa cuando una vela cerrada toca por primera vez la EMA 200.
2. **UT Bot + filtro EMA200**: LONG solo si Buy y precio sobre la EMA200; SHORT solo si Sell y precio bajo
   la EMA200; aviso "SALIR AHORA" con la señal contraria y resultado en %.
3. **Radar de candidatas**: en cada escaneo, top N alcistas y top N bajistas con mas opciones de explotar.
   - Puntuacion 0-100: compresion de bandas (30) + tiempo comprimida (15) + squeeze liberado (10)
     + volumen relativo (20) + cercania a EMA200 (10) + fuerza del momentum (15).
   - Direccion por 5 votos (momentum, aceleracion, EMA200, media 20, ruptura de rango). Minimo 3 votos.
   - Estados: 💥 DISPARADA, ⏳ COMPRIMIDA, ↔️ ESTRECHA.
   - Se evalua cada pick a RADAR_EVAL_SCANS velas y el resumen diario muestra su % de acierto.
4. **Resumen diario** (23:59 Europe/Madrid): ganadoras/perdedoras, total, por timeframe y calidad del radar.

## Filtros de calidad
Antes una señal UT que cumpliera "Buy y precio sobre la EMA200" abría posicion. Ahora debe pasar filtros duros y una puntuacion 0-100.
- **Filtros duros UT**: ADX >= 18 (no operar rango), volumen de la vela >= 1x la media, no pegada a la EMA200 (< 0.3 ATR),
  no muy extendida (> 8 ATR), vela de señal no excesiva (> 3 ATR) y como maximo 4 cruces UT en 40 velas (mas = mercado lateral).
- **Puntuacion UT**: ADX (20) + pendiente EMA200 a favor (15) + volumen (15) + distancia a la EMA (15) + vela de impulso (10)
  + pocos cruces (10) + tendencia EMA9/21 en 3 TF superiores (15). Si BTC va en contra resta 10 (o bloquea con BTC_BLOCK=true).
- **Toque EMA200**: solo rebotes (la vela cierra del lado de origen con mecha >= 35%), puntuados por ADX, pendiente, volumen, mecha y TF superiores.
- **Radar**: una DISPARADA necesita RVOL >= 1.3x, las candidatas deben tener TF superiores a favor y ir contra BTC resta puntos.
- **Resumen diario**: resultado por tramo de calidad (para comprobar que las mejores señales rinden mas) y lista de señales descartadas por cada filtro.
- Compatibilidad: `UT_MIN_QUALITY=0`, `UT_MIN_ADX=0`, `UT_MIN_RVOL=0`, `UT_MIN_EMA_DIST_ATR=0`, `UT_MAX_FLIPS=99`, `USE_HTF=false`, `USE_BTC_FILTER=false`,
  `TOUCH_MIN_QUALITY=0`, `TOUCH_ONLY_REJECTION=false` reproducen el comportamiento anterior.

## Despliegue (Railway)
1. Sube todos los archivos a GitHub (en la raiz).
2. Railway -> Deploy from GitHub repo.
3. Settings -> Custom Start Command: `python bot.py`
4. Variables: TELEGRAM_TOKEN y TELEGRAM_CHAT_ID (resto opcional, ver .env.example).

## Notas
- El estado (posiciones virtuales, historial del dia) vive en memoria: un reinicio lo borra.
- La compresion de volatilidad indica que se acerca una expansion, no su direccion.
- Informacion general, no es asesoramiento de inversion.
