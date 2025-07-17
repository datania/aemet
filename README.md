# Datos AEMET ⛅

Datos de la API de AEMET, a un archivo de distancia. Este repositorio contiene scripts pare exportar datos de algunos de los endpoints disponibles en la [API de AEMET](https://opendata.aemet.es/dist/index.html).


## 🛠️ Configuración

Para ejecutar los scripts en local, necesitas tener `uv` y la variable `AEMET_API_TOKEN` cargada en el entorno. Puedes instalar las dependencias con el siguiente comando.

```bash
make setup
```

Puedes leer algunas preguntas frecuentes del servicio para saber más sobre la [obtención de la clave y otras cosas](https://opendata.aemet.es/centrodedescargas/docs/FAQs170621.pdf).

## 🚀 Uso

Si quieres ejecutar el script, basta con hacer:

```bash
make run
```

Puedes checkear `data/raw` mientras que se ejecuta para ver los archivos descargados.

## 📄 Licencia

MIT.
