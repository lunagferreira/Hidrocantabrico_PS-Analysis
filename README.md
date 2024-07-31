# HIDROCANTABRICO_PS Analysis

## Overview

This project analyzes data from the "HIDROCANTABRICO_PS" dataset. It was created to process and extract insights from the data using Scala for data processing and analysis. The tasks include cleaning the data, performing statistical analysis, and generating visualizations to understand the patterns and trends within the dataset. Recently, additional features were added to enhance data accuracy by decoding certain fields using supplementary codification files.

## Dataset

The project utilizes multiple data files from the "HIDROCANTABRICO_PS" dataset. The dataset includes detailed records related to power supply points, their locations, tariffs, and other relevant attributes.

### Files Included:
- `HIDROCANTABRICO_PS_03140_001`
- `HIDROCANTABRICO_PS_03140_003`
- `HIDROCANTABRICO_PS_03140_004`
- `HIDROCANTABRICO_PS_03140_005`
- `HIDROCANTABRICO_PS_03140_006`
- `HIDROCANTABRICO_PS_03140_011`
- `HIDROCANTABRICO_PS_03140_012`
- `HIDROCANTABRICO_PS_03200_001`
- `HIDROCANTABRICO_PS_03200_003`
- `HIDROCANTABRICO_PS_03200_004`
- `HIDROCANTABRICO_PS_03200_005`
- `HIDROCANTABRICO_PS_03200_012`
- `HIDROCANTABRICO_PS_33900_001`
- `HIDROCANTABRICO_PS_33900_003`
- `HIDROCANTABRICO_PS_33900_004`
- `HIDROCANTABRICO_PS_33900_005`
- `HIDROCANTABRICO_PS_33900_006`
- `HIDROCANTABRICO_PS_33900_011`
- `HIDROCANTABRICO_PS_50290_011`

## Additional Codification Files
- `CodificacionCampoProvincia.txt`
- `CodificacionCamposPropiedadEquipoMedida.txt`
- `CodificacionCamposPropiedadICP.txt`
- `CodificacionCamposTelegestionado.txt`
- `CodificacionCampoDisponibilidadICP.txt`

## Features

1. **Data Cleaning**: Processes the raw dataset to remove noise and inconsistencies.
2. **Data Decoding**: Utilizes additional codification files to replace coded fields with their corresponding descriptive values.
3. **Statistical Analysis**: Identifies trends and patterns within the data, such as power usage, peak demand periods, and geographical distribution of power supply points.
4. **Visualizations**: Charts and graphs to visualize the data and findings, making it easier to interpret the analysis results.

## Technologies Used

- **Scala**: For data processing and analysis.
- **Apache Spark**: For handling large-scale data processing.
- **Pandas**: For data manipulation and preprocessing.
- **Matplotlib**: For creating visualizations.
