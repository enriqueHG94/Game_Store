# Proyecto de ELT con PySpark y Snowflake

## Contenido
- [Descripción General](#descripción-general)
- [Configuración Inicial](#configuración-inicial)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Zona de Bronce](#zona-de-bronce)
- [Zona de Plata](#zona-de-plata)
- [Zona de Oro](#zona-de-oro)
- [Casos de Uso](#casos-de-uso)
- [Conclusión](#conclusión)

## Descripción General
Este proyecto de ELT (Extract, Load, and Transform) utiliza Python 3.10.0, PySpark 3.3.3 y Snowflake para procesar y analizar grandes conjuntos de datos. Siguiendo una arquitectura de medallón, este proyecto divide los datos en zonas de Bronce, Plata y Oro, optimizando cada etapa del proceso para la eficiencia y el análisis efectivo. El proyecto se centra en el análisis de datos de una tienda de juegos, con un enfoque particular en la gestión del inventario, ventas y envíos.

## Configuración Inicial
El proyecto comienza con la configuración del entorno de desarrollo, incluyendo la instalación de Python y PySpark, y la configuración de las credenciales de Snowflake en un archivo `credentials.py`. Este enfoque garantiza la seguridad de los datos sensibles y la integración efectiva de las herramientas utilizadas.

## Estructura del Proyecto
El proyecto se organiza en varias carpetas que reflejan las diferentes fases del proceso de ELT:
- `bronze`: Contiene los archivos CSV originales, que representan los datos en su estado más crudo.
- `silver`: Incluye scripts de PySpark para transformar los datos de Bronce, preparándolos para análisis más detallados.
- `gold`: Aloja scripts para la creación del modelo dimensional en Snowflake, facilitando el análisis avanzado y la generación de informes.
- `jars`: Contiene las dependencias necesarias para la integración de PySpark con Snowflake, crucial para la conexión y manipulación de datos en Snowflake.

## Zona de Bronce
En esta etapa, se cargan datos crudos desde archivos CSV directamente a Snowflake, utilizando `extract_and_load.py`. Este proceso asegura que los datos se mantengan en su forma original antes de cualquier transformación, preservando su integridad.

Orden de Ejecución:
1. `tables_bronze.sql`: Crea tablas en Snowflake para cada CSV.
2. `extract_and_load.py`: Carga los datos en Snowflake.

## Zona de Plata
Aquí, los datos de Bronce se transforman y normalizan para su uso en análisis. Utilizando scripts como `trf_customers.py` y `trf_stores.py`, los datos se limpian, se calculan métricas adicionales, y se reformatean para mejorar su utilidad. Las transformaciones incluyen la estandarización de formatos, como números de teléfono y fechas, y la derivación de nuevas columnas para enriquecer los conjuntos de datos.

Orden de Ejecución:
1. Ejecutar `tables_silver.sql` para crear estructuras de tabla en la zona de plata.
2. Ejecutar scripts PySpark (ej., `trf_customers.py`) para cada conjunto de datos, que transforman y cargan los datos en las tablas de Plata.
3. ejecutar `new_row.sql` para añadir una nueva promoción a la tabla promociones.

## Zona de Oro
La fase final del proyecto implica la creación de un modelo dimensional, que estructura los datos para un análisis eficiente. Se utilizan scripts como `dim_stores.py` y `dim_employees.py` para cargar datos en este modelo, creando así un entorno adecuado para el análisis de negocios y la toma de decisiones.

Orden de Ejecución:
1. `tables_gold.sql` para crear estructuras de tabla en la zona de oro.
2. Scripts PySpark (ej., `dim_stores.py`, `dim_employees.py`) para cargar datos en las tablas de Oro.

## Casos de Uso
Se desarrollaron casos de uso específicos para demostrar la aplicabilidad de los datos transformados en la zona de oro:
- **Análisis de Inventario Bajo (`fct_inventory`)**: Identifica productos con stock bajo en las tiendas, ayudando a prevenir escasez.
- **Análisis de Ventas (`fct_sales`)**: Examina el volumen de ventas totales, los productos más vendidos y el rendimiento de los empleados en ventas, proporcionando insights valiosos sobre la eficacia de las estrategias de ventas.
- **Análisis de Envíos (`fct_shipments`)**: Evalúa la eficiencia de las compañías de envío y analiza los costos y la puntualidad de los envíos.

## Conclusión
Este proyecto demuestra un uso eficiente de Python, PySpark y Snowflake para el procesamiento y análisis de datos. Desde la carga inicial de datos hasta el análisis avanzado en la zona Gold, el proyecto abarca todas las fases necesarias para convertir datos en bruto en insights valiosos, facilitando así la toma de decisiones informada y estratégica.

## contacto
- Email: enriquehervasguerrero@gmail.com
- LinkedIn: https://www.linkedin.com/in/enrique-hervas-guerrero/
