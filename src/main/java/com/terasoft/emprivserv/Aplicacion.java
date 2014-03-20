package com.terasoft.emprivserv;

import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.viralpatel.java.CSVLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class Aplicacion {

	private static Log log = LogFactory.getLog("TestJdbc");
	
	public static DriverManagerDataSource dataSource;
	public static JdbcTemplate jdbcTemplate;
	public static Connection conn;  
	
	public static void main(String[] args) throws Exception {
	
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		
		dataSource = (DriverManagerDataSource) context.getBean("dataSource");
		jdbcTemplate = (JdbcTemplate) context.getBean("jdbcTemplate");
		conn = jdbcTemplate.getDataSource().getConnection();
		
		/* EJEMPLO DE FUNCIONAMIENTO
		importarInforme(201208, "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/BH+/BH201208+.csv");
		genSuiAcu(201208);
		genCsvAcu(201208);
		exportarCsv(201208, "ACU", "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/CSV/acu%s.csv");
		*/
		
		//importarInforme(201311, "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/BH+/BH201311+.csv");
		//importarInforme(201312, "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/BH+/BH201312+.csv");
		
		List<Integer> periodos = new ArrayList<Integer>();
		//periodos.add(201311);
		periodos.add(201312);
		for (Integer periodo : periodos) {
			genSuiAcu(periodo);
			genCsvAcu(periodo);
			exportarCsv(periodo, "ACU", "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/CSV/acu%s.csv");
			
			genSuiAlc(periodo);
			genCsvAlc(periodo);
			exportarCsv(periodo, "ALC", "/Volumes/Datos/TRABAJO/EmpPrivServ/INFORME/CSV/alc%s.csv");
		}
	}

	public static void importarInforme(Integer periodo, String archivo) throws Exception {
		log.info("importarInforme " + periodo + " " + archivo + " CSV -> DB");
		CSVLoader csvLoader = new CSVLoader(conn);
		csvLoader.loadCSV(archivo, "INFORME_TMP_V2", true);
		
		log.info("importarInforme " + periodo + " " + archivo + " pasaInforme_v2()");
		CallableStatement statement = conn.prepareCall("{call pasaInforme_v2(?)}");
		statement.setInt(1, periodo);	
		statement.executeUpdate();	
	}

	
	/** Genera el informe SUI de Acueducto
	 * @param periodo Perido en formato AAAAMM
	 * @throws Exception
	 */
	public static void genSuiAcu(Integer periodo) throws Exception {
		System.out.print("genSuiAcu(" + periodo + ")");
		CallableStatement statement = conn.prepareCall("{call genSuiAcu(?, ?, ?)}");
		statement.setInt(1, periodo);
		statement.setInt(2, (Integer) periodo / 100);
		statement.setInt(3, (Integer) periodo % 100);
		statement.executeUpdate();
		log.info(" - OK");
	}

	/** Genera el informe SUI de Alcantarillado
	 * @param periodo Perido en formato AAAAMM
	 * @throws Exception
	 */
	public static void genSuiAlc(Integer periodo) throws Exception {
		System.out.print("genSuiAlc(" + periodo + ")");
		CallableStatement statement = conn.prepareCall("{call genSuiAlc(?, ?, ?)}");
		statement.setInt(1, periodo);
		statement.setInt(2, (Integer) periodo / 100);
		statement.setInt(3, (Integer) periodo % 100);
		statement.executeUpdate();
		log.info(" - OK");
	}

	/** Genera las lineas formateadas como CSV para Acueducto en la tabla PLANOCSV
	 * @param periodo a procesar
	 * @throws Exception
	 */
	public static void genCsvAcu(Integer periodo) throws Exception {
		System.out.print("genCsv2010Acu(" + periodo + ")");
		CallableStatement statement = conn.prepareCall("{call genCsv2010Acu(?)}");
		statement.setInt(1, periodo);
		statement.executeUpdate();
		log.info(" - OK");
	}

	/** Genera las lineas formateadas como CSV para Acueducto en la tabla PLANOCSV
	 * @param periodos Lista de periodos a procesar
	 * @throws Exception
	 */
	public static void genCsvAlc(Integer periodo) throws Exception {
		System.out.print("genCsv2010Alc(" + periodo + ")");
		CallableStatement statement = conn.prepareCall("{call genCsv2010Alc(?)}");
		statement.setInt(1, periodo);
		statement.executeUpdate();
		log.info(" - OK");
	}

	/**
	 * @param periodos Lista de periodos a procesar
	 * @throws Exception
	 */
	public static void exportarCsv(Integer periodo, String servicio, String maskFile) throws Exception {
		Statement stm = null;
		PrintWriter writer = null;
		
		String archivo = String.format(maskFile, periodo);
		System.out.print("exportarCsv " + periodo + " => " + archivo);
		
		try {
			writer = new PrintWriter(archivo, "ISO-8859-1");
			stm = conn.createStatement();
			ResultSet rs = stm.executeQuery(String.format("select linea from planocsv where periodo = %s and formato = '%s'", periodo, servicio));
			while (rs.next()) {
				writer.print(rs.getString("linea"));
				writer.print("\r\n");
			}
		} finally {
			if (stm != null) stm.close();
			if (writer != null) writer.close();
			log.info(" - OK");
		}
	}
	
}
