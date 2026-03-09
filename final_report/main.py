import pandas as pd
import glob
import os
from datetime import datetime

# ESTELY!! YO TRABAJO CON ENV LOCALES SEGUN PROYECTO, POR FA EJECUTA: python3 -m venv env 

# --- ESTELY, DEJO ESTO PORQUE ES PARA CONFIGURACION SI SE PUEDE MEJORAR, HAZLO... CONFIGURACIÓN ---
ARCHIVO_BLOQUES = 'ce_bloques.csv'
BLOQUES_INTERES = ['B1', 'B2']
DOMINIO_ESTUDIANTE = '@clases.edu.sv'
BLACKLIST = ['test', 'tutor', 'demo', 'prueba', 'admin']
FECHA_REPORTE = datetime.now().strftime("%d/%m/%Y")
ARCHIVO_SALIDA = f'Reporte_final_{datetime.now().strftime("%Y%m%d")}.xlsx'
COLS_DETALLE = ['Grade', 'Track', 'Subtrack', 'Subject', 'Class_period']

def main():
    print(f"🚀 INICIANDO REPORTE V16 (REGLAS DE NEGOCIO STRICT MODE)...")

    # =========================================================================
    # 1. CARGA DE BLOQUES: AQUI LEE LA INFO DEL ARCHIVO DE BLOQUES, LO PUEDES EDITAR
    # EN LA VARIABLE DE ARRIBA :D TQM
    # =========================================================================
    try:
        df_bloques = pd.read_csv(ARCHIVO_BLOQUES, dtype=str)
        df_bloques.columns = df_bloques.columns.str.strip()
        df_target = df_bloques[df_bloques['GRUPO (BLOQUE)'].isin(BLOQUES_INTERES)].copy()
        
        codigos_vip = df_target['CODIGO'].unique()
        mapa_bloques = dict(zip(df_target['CODIGO'], df_target['GRUPO (BLOQUE)']))
        mapa_grupo_num = {'B1': '1', 'B2': '2'} 
        print(f"✅ Bloques cargados. Escuelas B1/B2: {len(codigos_vip)}")
    except Exception as e:
        print(f"❌ Error crítico leyendo bloques ({ARCHIVO_BLOQUES}): {e}")
        return

    # =========================================================================
    # 2. PROCESAMIENTO DE DOCENTES: IGUAL QUE EL DE BLOQUES... TQM X2
    # =========================================================================
    print("--- Procesando Docentes ---")
    files_t = glob.glob('teacher_cumulative_group_*.csv')
    
    df_t_unique = pd.DataFrame()
    stats_t = pd.DataFrame(columns=['Code'])
    stats_secciones = pd.DataFrame(columns=['Code'])
    ########
    df_hoja6 = pd.DataFrame()
    df_hoja7 = pd.DataFrame()
    ########
    if files_t:
        try:
            df_t = pd.read_csv(files_t[0], dtype=str)
            df_t.columns = df_t.columns.str.strip()

            ########Parte Nueva: ESTO ES PARA QUE CARGUE POR BLOQUES LAS LISTAS QUE PIDA LAU#########
            # BASE TAL CUAL SE CARGA EL CSV (MISMAS COLUMNAS), SOLO PARA FILTRAR A B1 ---
            df_t_cargado = df_t.copy()
            df_t_cargado2= df_t.copy()
            # Cruzar con ce_bloques.csv vía mapa_bloques (Code -> GRUPO (BLOQUE)) y filtrar SOLO B1
            df_hoja6 = (
            df_t_cargado[df_t_cargado['Code'].map(mapa_bloques) == 'B1']
            .drop_duplicates()
            .copy()
            )

            df_hoja7 = (
            df_t_cargado2[df_t_cargado2['Code'].map(mapa_bloques) == 'B2']
            .drop_duplicates()
            .copy()
            )
            ############################

            df_t = df_t[df_t['Code'].isin(codigos_vip)]
            
            # REGLA DE NEGOCIO: Solo "Clase"
            if 'Type' in df_t.columns:
                df_t = df_t[df_t['Type'].str.strip() == 'Clase']
                
            df_t['Access_status'] = pd.to_numeric(df_t['Access_status'], errors='coerce').fillna(0).astype(int)
            df_t['Email'] = df_t['Email'].str.lower().str.strip()

            # --- SECCIONES (Calculado sobre data filtrada) ---
            cols_seccion = ['Code', 'School_name', 'Type', 'Grade', 'Track', 'Subtrack', 'Class', 'Subject', 'Class_period']
            cols_existentes = [c for c in cols_seccion if c in df_t.columns]
            if cols_existentes:
                df_secciones = df_t.groupby(cols_existentes)['Access_status'].max().reset_index()
                stats_secciones = df_secciones.groupby('Code').agg(
                    total_secciones=('Access_status', 'count'),
                    total_secciones_access=('Access_status', 'sum')
                ).reset_index()

            # --- DOCENTES ÚNICOS ---
            # Prioridad Acceso=1 para la desduplicación
            df_t = df_t.sort_values(by=['Code', 'Access_status'], ascending=[True, False])
            df_t_unique = df_t.drop_duplicates(subset=['Code', 'Email'], keep='first').copy()
            
            df_t_unique['is_demo'] = df_t_unique['Email'].str.contains('demo', na=False)
            df_t_unique['is_real'] = ~df_t_unique['is_demo']
            df_t_unique['has_access'] = (df_t_unique['is_real']) & (df_t_unique['Access_status'] == 1)

            stats_t = df_t_unique.groupby(['Code']).agg(
                School_name=('School_name', 'first'),
                total_teacher=('Email', 'count'),
                total_demo_teacher=('is_demo', 'sum'),
                total_teacher_access=('has_access', 'sum')
            ).reset_index()
            # REGLA DE NEGOCIO: Reales = Total - Demos
            stats_t['total_teacher_reales'] = stats_t['total_teacher'] - stats_t['total_demo_teacher']
            print(f"   - Docentes procesados: {len(stats_t)}")
        except Exception as e:
            print(f"⚠️ Alerta en Docentes: {e}")

    # =========================================================================
    # 3. PROCESAMIENTO DE ESTUDIANTES
    # =========================================================================
    print("--- Procesando Estudiantes ---")
    files_s = glob.glob('student_cumulative_unique_*.csv')
    
    df_s_unique = pd.DataFrame()
    stats_s = pd.DataFrame(columns=['Code'])

    if files_s:
        try:
            df_s = pd.read_csv(files_s[0], dtype=str)
            df_s.columns = df_s.columns.str.strip()
            df_s = df_s[df_s['Code'].isin(codigos_vip)]
            
            # REGLA DE NEGOCIO: Dominio Estricto
            if 'Email' in df_s.columns:
                df_s['Email'] = df_s['Email'].str.lower().str.strip()
                df_s = df_s[df_s['Email'].str.contains(DOMINIO_ESTUDIANTE, na=False)]

            # REGLA DE NEGOCIO: Blacklist (Eliminar pruebas y tutores)
            mask_basura = pd.Series(False, index=df_s.index)
            if 'Class' in df_s.columns:
                for w in BLACKLIST: mask_basura |= df_s['Class'].astype(str).str.lower().str.contains(w, na=False)
            if 'Name' in df_s.columns:
                for w in BLACKLIST: mask_basura |= df_s['Name'].astype(str).str.lower().str.contains(w, na=False)
            
            n_basura = mask_basura.sum()
            df_s = df_s[~mask_basura] # Aplicamos filtro
            print(f"   - Registros basura eliminados (Blacklist): {n_basura}")

            df_s['Access_status'] = pd.to_numeric(df_s['Access_status'], errors='coerce').fillna(0).astype(int)
            df_s = df_s.sort_values(by=['Code', 'Access_status'], ascending=[True, False])
            
            # Unicidad
            df_s_unique = df_s.drop_duplicates(subset=['Code', 'Email'], keep='first').copy()

            stats_s = df_s_unique.groupby(['Code']).agg(
                total_students=('Email', 'count'),
                total_student_access=('Access_status', 'sum')
            ).reset_index()
            print(f"   - Estudiantes procesados: {len(stats_s)}")
        except Exception as e:
             print(f"⚠️ Alerta en Estudiantes: {e}")

    # =========================================================================
    # 4. TABLA MAESTRA: ESTOS SON LOS REPORTES PARA EL DASHBOARD
    # =========================================================================
    print("--- Generando Excel Final ---")
    master = stats_t.merge(stats_s, on='Code', how='outer')
    master = master.merge(stats_secciones, on='Code', how='left')

    required_cols = ['total_students', 'total_student_access', 'total_teacher', 'total_demo_teacher', 
                     'total_teacher_reales', 'total_teacher_access', 'total_secciones', 'total_secciones_access']
    for c in required_cols:
        if c not in master.columns: master[c] = 0 
        master[c] = master[c].fillna(0).astype(int)

    master['Bloque'] = master['Code'].map(mapa_bloques).fillna('Sin Bloque')
    
    if 'School_name_x' in master.columns:
        master['School_name'] = master['School_name_x'].fillna(master['School_name_y'] if 'School_name_y' in master.columns else 'Desconocida')
    elif 'School_name' in master.columns:
        master['School_name'] = master['School_name'].fillna('Desconocida')
    else:
        master['School_name'] = 'Desconocida'

    cols_order = ['Code', 'School_name', 'Bloque'] + required_cols
    df_hoja2 = master[[c for c in cols_order if c in master.columns]].copy()

    # =========================================================================
    # 5. RESUMEN BLOQUES (HOJA 1)
    # =========================================================================
    df_hoja1 = df_hoja2.groupby('Bloque')[required_cols].sum().reset_index()
    df_hoja1 = df_hoja1[df_hoja1['Bloque'].isin(['B1', 'B2'])]
    
    # Orden solicitado + Reales
    cols_h1 = ['Bloque', 
               'total_teacher', 
               'total_demo_teacher', 
               'total_teacher_reales', # ✅ Aquí está tu columna sagrada
               'total_teacher_access',
               'total_students', 
               'total_student_access', 
               'total_secciones', 
               'total_secciones_access']
    df_hoja1 = df_hoja1[cols_h1]

    # =========================================================================
    # 6. SUPABASE (HOJA 3): PARA SUBIR A METRICS PERO YA NO ES NECESRIA :D 
    # RECUERDA: SI FUNCIONA, NO LO TOQUES!! JAJAJAJA
    # =========================================================================
    supabase_rows = []
    for _, row in df_hoja1.iterrows():
        bloque_num = mapa_grupo_num.get(row['Bloque'], '0')
        
        # Docentes: Usamos REALES
        supabase_rows.append({'total': row['total_teacher_reales'], 'access': row['total_teacher_access'], 'demo': row['total_demo_teacher'],
                              'type': 'Docentes', 'dateReported': FECHA_REPORTE, 'group': bloque_num, 'category': 'Acumulado'})
        # Estudiantes
        supabase_rows.append({'total': row['total_students'], 'access': row['total_student_access'], 'demo': 0,
                              'type': 'Estudiantes', 'dateReported': FECHA_REPORTE, 'group': bloque_num, 'category': 'Acumulado'})
        # Secciones
        supabase_rows.append({'total': row['total_secciones'], 'access': row['total_secciones_access'], 'demo': int(row['total_secciones']) - int(row['total_secciones_access']),
                              'type': 'Secciones', 'dateReported': FECHA_REPORTE, 'group': bloque_num, 'category': 'Acumulado'})
    df_hoja3 = pd.DataFrame(supabase_rows)

    # =========================================================================
    # 7. LISTAS CON DETALLE (HOJA 4 y 5)
    # =========================================================================
    df_hoja4 = pd.DataFrame()
    df_hoja5 = pd.DataFrame()

    if not df_t_unique.empty:
        # Extraemos columnas de detalle si existen
        cols_finales_t = ['Code', 'Name', 'Email'] + [c for c in COLS_DETALLE if c in df_t_unique.columns]
        df_hoja4 = df_t_unique[(df_t_unique['is_real']) & (df_t_unique['Access_status'] == 0)][cols_finales_t].copy()
        df_hoja4['Bloque'] = df_hoja4['Code'].map(mapa_bloques)

    if not df_s_unique.empty:
        cols_finales_s = ['Code', 'Name', 'Email'] + [c for c in COLS_DETALLE if c in df_s_unique.columns]
        # Aseguramos Name/Email
        if 'Name' not in cols_finales_s and 'Name' in df_s_unique.columns: cols_finales_s.insert(1, 'Name')
        if 'Email' not in cols_finales_s and 'Email' in df_s_unique.columns: cols_finales_s.insert(2, 'Email')

        df_hoja5 = df_s_unique[df_s_unique['Access_status'] == 0][cols_finales_s].copy()
        df_hoja5['Bloque'] = df_hoja5['Code'].map(mapa_bloques)

    try:
        with pd.ExcelWriter(ARCHIVO_SALIDA, engine='xlsxwriter') as writer:
            df_hoja1.to_excel(writer, sheet_name='Resumen_Bloques', index=False)
            df_hoja2.to_excel(writer, sheet_name='Detalle_Centros', index=False)
            df_hoja3.to_excel(writer, sheet_name='Carga_Supabase', index=False)
            df_hoja4.to_excel(writer, sheet_name='Docentes_Sin_Acceso', index=False)
            df_hoja5.to_excel(writer, sheet_name='Estudiantes_Sin_Acceso', index=False)
            # Nueva hoja 6 con carga académica B1:
            df_hoja6.to_excel(writer, sheet_name="Carga_Académica_Grupo1", index=False)
            df_hoja7.to_excel(writer, sheet_name="Carga_Académica_Grupo2", index=False)
            
            for sheet in writer.sheets.values():
                sheet.set_column(0, 15, 20)

        print(f"🎉 ¡ÉXITO! Reporte V16 generado: {ARCHIVO_SALIDA}")
    except Exception as e:
        print(f"❌ Error guardando Excel: {e}")

if __name__ == "__main__":
    main()