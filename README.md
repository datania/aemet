# Datos AEMET ⛅

Datos de la API de AEMET, a un archivo de distancia. Este repositorio contiene scripts pare exportar datos de algunos de los endpoints disponibles en la [API de AEMET](https://opendata.aemet.es/dist/index.html).


## 🛠️ Configuración

Para ejecutar el scripts en local, necesitas tener `uv` y la variable `AEMET_API_TOKEN` cargada en el entorno. Puedes leer algunas preguntas frecuentes del servicio para saber más sobre la [obtención de la clave y otras cosas](https://opendata.aemet.es/centrodedescargas/docs/FAQs170621.pdf).

## 🚀 Uso

Si quieres ejecutar el script, basta con hacer:

```bash
make estaciones
make valores-climatologicos
```

También puede ejecutarlo directamente.

```bash
usage: aemet [-h] {estaciones,valores-climatologicos} ...

Export AEMET climate data

positional arguments:
  {estaciones,valores-climatologicos}
                        Available commands
    estaciones          Fetch and save station information
    valores-climatologicos
                        Fetch and save climate data

options:
  -h, --help            show this help message and exit

Examples:
  aemet estaciones -o data
  aemet valores-climatologicos --start 2025-01-01 --end 2025-01-31 -o data
```

Checkea `data/raw` mientras que se ejecuta para ver los archivos descargados.

## 📄 Licencia

MIT.
