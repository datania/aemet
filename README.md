# Datos AEMET

Algunos datos de la API de AEMET, a un archivo de distancia!

Si tienes `uv` instalado, puedes ejecturar directamente el script `./aemet`.

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
