# test_gspread_app.py
import streamlit as st
import json
import gspread
from google.oauth2.service_account import Credentials

st.title("Test de Conexión a Google Sheets")

def get_google_sheets_client():
    try:
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Error de autenticación: {e}")
        st.stop()

try:
    gc_test = get_google_sheets_client()
    st.success(f"Cliente de gspread inicializado correctamente. Tipo: {type(gc_test)}")
    
    # Intenta abrir una hoja de cálculo
    sheet_id = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY' # TU ID DE HOJA
    spreadsheet_test = gc_test.open_by_key(sheet_id)
    st.success(f"Hoja de cálculo '{sheet_id}' abierta exitosamente. Título: {spreadsheet_test.title}")
    
    # Intenta obtener una hoja de trabajo (worksheet)
    worksheet_name = 'datos_pedidos' # TU NOMBRE DE WORKSHEET
    worksheet_test = spreadsheet_test.worksheet(worksheet_name)
    st.success(f"Hoja de trabajo '{worksheet_name}' obtenida exitosamente. Filas: {worksheet_test.row_count}")
    
    # Intenta leer algunas celdas
    data = worksheet_test.get_all_values()
    st.write("Primeras 5 filas de datos:", data[:5])
    
except Exception as e:
    st.error(f"❌ Error durante la operación con Google Sheets: {e}")
    st.info("Asegúrate de que el ID de la hoja y el nombre de la pestaña son correctos y que la cuenta de servicio tiene permisos.")
