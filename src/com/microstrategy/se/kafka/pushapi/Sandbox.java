/**
* Configuration class for the MicroStrategySink Kafka connector
*
* @author  Alex Fernandez
* @version 0.1
* @since   2018-08-01 
*
*/

package com.microstrategy.se.kafka.pushapi;

public class Sandbox {

	public static void main(String[] args) throws Exception {
		
		String libraryUrl = "https://xxx.microstrategy.com/MicroStrategyLibrary";
		String username = "username";
		String password = "password";
		
		MicroStrategy mstr = new MicroStrategy(libraryUrl, username, password);
		mstr.connect();
		mstr.setProject("Tutorial Project");

	}

}
