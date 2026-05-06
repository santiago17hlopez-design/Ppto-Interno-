#PPto Interno

import pandas as pd
import os


class PptoInterno:

    def __init__(self, promotora):
        try:
            self.promotora = promotora

            self.ruta_origen = os.path.join(
                "data",
                f"promoter_{self.promotora}",
                "input",
                "file.xlsx"
            )

            self.ruta_salida = os.path.join(
                "data",
                f"promoter_{self.promotora}",
                "output"
            )

            self.df = None
            self.df_procesado = None

            self.columnas_requeridas = [
                "CodDirector",
                "Tipo",
                "CodAgente",
                "Agente Comercial",
                "CodRamo",
                "Ramo",
                "Fecha",
                "Valor",
                "CodOficinaU",
                "Tasa",
                "Presupuesto Honorarios"
            ]

            self.PROMOTORAS = {
                "Promoter A": "0001",
                "Promoter B": "0002",
                "Promoter C": "0003",
                "Promoter D": "0004",
                "Promoter E": "0005"
            }

        except Exception as e:
            raise Exception(f"Error en inicialización: {e}")

    def leer_excel(self):
        """Lee el archivo Excel. Retorna True si tuvo éxito, False si el archivo no existe."""
        if not os.path.exists(self.ruta_origen):
            print("⚠️ El archivo origen no existe.")
            return False

        try:
            self.df = pd.read_excel(self.ruta_origen, sheet_name="Sheet1", dtype=str)
            print("✅ Archivo leído correctamente.")
            return True
        except Exception as e:
            print(f"❌ Error al leer el archivo: {e}")
            return False

    def hacer_pivot(self):
        """Transforma las columnas de meses en filas."""
        try:
            if self.df is None or self.df.empty:
                return

            columnas_fijas = [
                "CodDirector",
                "Tipo",
                "CodAgente",
                "Agente Comercial",
                "CodRamo",
                "Ramo"
            ]

            columnas_meses = self.df.columns[6:]

            self.df_procesado = self.df.melt(
                id_vars=columnas_fijas,
                value_vars=columnas_meses,
                var_name="Fecha",
                value_name="Valor"
            )

            print("✅ Pivot realizado.")

        except Exception as e:
            raise Exception(f"Error en el pivot: {e}")

    def crear_CodOficinaU(self):
        """Crea la columna 'CodOficinaU'."""
        try:
            if self.df_procesado is None or self.df_procesado.empty:
                return

            codigo_p = self.PROMOTORAS.get(self.promotora, "")

            if codigo_p != "" and "CodAgente" in self.df_procesado.columns:
                self.df_procesado["CodOficinaU"] = (
                    codigo_p + self.df_procesado["CodAgente"].astype(str)
                )

                cols = ["CodOficinaU"] + [
                    c for c in self.df_procesado.columns if c != "CodOficinaU"
                ]

                self.df_procesado = self.df_procesado[cols]

                print("✅ Columna 'CodOficinaU' creada.")

        except Exception as e:
            raise Exception(f"Error al crear CodOficinaU: {e}")

    def aplicar_formatos(self):
        """Limpia y formatea tipos de datos."""
        try:
            if self.df_procesado is None or self.df_procesado.empty:
                return

            self.df_procesado["Fecha"] = pd.to_datetime(
                self.df_procesado["Fecha"],
                dayfirst=True,
                errors="coerce"
            )

            self.df_procesado["Fecha"] = self.df_procesado["Fecha"].dt.strftime("%d/%m/%Y")

            self.df_procesado["Valor"] = (
                pd.to_numeric(self.df_procesado["Valor"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

            print("✅ Formatos aplicados.")

        except Exception as e:
            print(f"⚠️ Nota: No se aplicaron formatos: {e}")

    def exportar(self):
        """Exporta a CSV, incluso si el DataFrame está vacío."""
        try:
            if not os.path.exists(self.ruta_salida):
                os.makedirs(self.ruta_salida)

            if self.df_procesado is None:
                self.df_procesado = pd.DataFrame(columns=self.columnas_requeridas)

            ruta_final = os.path.join(self.ruta_salida, "file.csv")

            self.df_procesado.to_csv(
                ruta_final,
                index=False,
                sep=";",
                encoding="utf-8-sig"
            )

            status = "VACÍO (solo cabeceras)" if self.df_procesado.empty else "con DATOS"

            print(f"✅ Archivo exportado {status}.")

        except Exception as e:
            raise Exception(f"Error al exportar CSV: {e}")

    def crear_ppto_honorarios(self):
        """Agrega la columna 'Presupuesto Honorarios' desde un archivo parquet."""
        try:
            if self.df_procesado is None or self.df_procesado.empty:
                print("⚠️ No hay datos procesados para calcular honorarios.")
                return self.df_procesado

            ruta_pago_final = os.path.join(
                "data",
                f"promoter_{self.promotora}",
                "output",
                "file.parquet"
            )

            if not os.path.exists(ruta_pago_final):
                print("⚠️ No se encontró el archivo parquet.")
                self.df_procesado["Presupuesto Honorarios"] = pd.NA
                return self.df_procesado

            df_tasas = pd.read_parquet(ruta_pago_final)

            if df_tasas is None or df_tasas.empty:
                print("⚠️ El archivo parquet está vacío.")
                self.df_procesado["Presupuesto Honorarios"] = pd.NA
                return self.df_procesado

            columnas_requeridas_tasas = ["CodAgente", "CodRamo", "Tasa"]

            faltantes = [
                c for c in columnas_requeridas_tasas
                if c not in df_tasas.columns
            ]

            if faltantes:
                raise Exception(f"Columnas faltantes en file.parquet: {faltantes}")

            def normalizar_texto(serie):
                return (
                    serie.astype(str)
                    .str.strip()
                    .str.replace(r"\s+", " ", regex=True)
                    .str.upper()
                )

            df_tasas = df_tasas[columnas_requeridas_tasas].copy()

            df_tasas["CodAgente"] = normalizar_texto(df_tasas["CodAgente"])
            df_tasas["CodRamo"] = normalizar_texto(df_tasas["CodRamo"])

            df_tasas["Tasa"] = pd.to_numeric(
                df_tasas["Tasa"]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip(),
                errors="coerce"
            )

            self.df_procesado["CodAgente"] = normalizar_texto(
                self.df_procesado["CodAgente"]
            )

            if "CodRamo" in self.df_procesado.columns:
                self.df_procesado["CodRamo"] = normalizar_texto(
                    self.df_procesado["CodRamo"]
                )

            df_tasas = df_tasas.drop_duplicates(
                subset=["CodAgente", "CodRamo"],
                keep="last"
            )

            print("MUESTRA PRESUPUESTO:")
            print(
                self.df_procesado[["CodAgente", "CodRamo"]]
                .drop_duplicates()
                .head(20)
            )

            print("MUESTRA TASAS:")
            print(
                df_tasas[["CodAgente", "CodRamo", "Tasa"]]
                .drop_duplicates()
                .head(20)
            )

            self.df_procesado = self.df_procesado.merge(
                df_tasas[["CodAgente", "CodRamo", "Tasa"]],
                on=["CodAgente", "CodRamo"],
                how="left",
                validate="m:1"
            )

            print(
                self.df_procesado[
                    self.df_procesado["Tasa"].isna()
                ][["CodAgente", "CodRamo"]]
                .drop_duplicates()
                .head(50)
            )

            self.df_procesado["Presupuesto Honorarios"] = (
                pd.to_numeric(self.df_procesado["Valor"], errors="coerce")
                * self.df_procesado["Tasa"]
            )

            faltantes_tasa = self.df_procesado["Tasa"].isna().sum()

            if faltantes_tasa:
                print(
                    f"⚠️ {faltantes_tasa} filas sin tasa de honorarios. "
                    "Se dejó NaN en 'Presupuesto Honorarios'."
                )
            else:
                print(
                    f"✅ 'Presupuesto Honorarios' agregada para "
                    f"{len(self.df_procesado)} filas."
                )

            return self.df_procesado

        except Exception as e:
            raise Exception(f"Error al crear tasas de honorarios: {e}")

    def ejecutar(self, crear_ppto_honorarios=None):
        """Flujo principal con validación de existencia de archivo."""
        try:
            archivo_existe = self.leer_excel()

            if archivo_existe:
                self.hacer_pivot()
                self.crear_CodOficinaU()
                self.aplicar_formatos()

                if self.df_procesado is not None:
                    cols_to_keep = [
                        c for c in self.columnas_requeridas
                        if c in self.df_procesado.columns
                    ]

                    self.df_procesado = self.df_procesado[cols_to_keep]

            else:
                print("ℹ️ Generando archivo de salida con estructura base.")
                self.df_procesado = pd.DataFrame(columns=self.columnas_requeridas)

            if crear_ppto_honorarios is True:
                self.crear_ppto_honorarios()

            self.exportar()

            print("🚀 Proceso finalizado.")

        except Exception as e:
            print(f"❌ Error crítico en ejecución: {e}")


if __name__ == "__main__":
    proceso = PptoInterno("Promoter A")
    proceso.ejecutar(crear_ppto_honorarios=True)
