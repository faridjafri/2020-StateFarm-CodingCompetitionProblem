package sf.codingcompetition2020;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import sf.codingcompetition2020.structures.Agent;
import sf.codingcompetition2020.structures.Claim;
import sf.codingcompetition2020.structures.Customer;
import sf.codingcompetition2020.structures.Vendor;

public class CodingCompCsvUtil {
	
	/* #1 
	 * readCsvFile() -- Read in a CSV File and return a list of entries in that file.
	 * @param filePath -- Path to file being read in.
	 * @param classType -- Class of entries being read in.
	 * @return -- List of entries being returned.
	 */
	public <T> List<T> readCsvFile(String filePath, Class<T> classType) {
		CsvMapper csvMapper = new CsvMapper();
		csvMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
		CsvSchema schema = CsvSchema.emptySchema().withHeader();
		ObjectReader oReader = csvMapper.reader(classType).with(schema);
		List<T> objects = new ArrayList<>();
		try (Reader reader = new FileReader(filePath)) {
			MappingIterator<T> mi = oReader.readValues(reader);
			while (mi.hasNext()) {
				objects.add(mi.next());
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return objects;
	}

	
	/* #2
	 * getAgentCountInArea() -- Return the number of agents in a given area.
	 * @param filePath -- Path to file being read in.
	 * @param area -- The area from which the agents should be counted.
	 * @return -- The number of agents in a given area
	 */
	public int getAgentCountInArea(String filePath, String area) {
		List<Agent> agents = readCsvFile(filePath, Agent.class);
		return agents.stream().filter(agent -> agent.getArea().equals(area)).collect(Collectors.toList()).size();
	}

	
	/* #3
	 * getAgentsInAreaThatSpeakLanguage() -- Return a list of agents from a given area, that speak a certain language.
	 * @param filePath -- Path to file being read in.
	 * @param area -- The area from which the agents should be counted.
	 * @param language -- The language spoken by the agent(s).
	 * @return -- The number of agents in a given area
	 */
	public List<Agent> getAgentsInAreaThatSpeakLanguage(String filePath, String area, String language) {
		List<Agent> agents = readCsvFile(filePath, Agent.class);
		return agents.stream().filter(agent -> agent.getArea().equals(area) && agent.getLanguage().equals(language))
				.collect(Collectors.toList());
	}
	
	
	/* #4
	 * countCustomersFromAreaThatUseAgent() -- Return the number of individuals from an area that use a certain agent.
	 * @param filePath -- Path to file being read in.
	 * @param customerArea -- The area from which the customers should be counted.
	 * @param agentFirstName -- First name of agent.
	 * @param agentLastName -- Last name of agent.
	 * @return -- The number of customers that use a certain agent in a given area.
	 */
	public short countCustomersFromAreaThatUseAgent(Map<String, String> csvFilePaths, String customerArea,
			String agentFirstName, String agentLastName) {
		AtomicInteger i = new AtomicInteger();
		List<Agent> agents = readCsvFile(csvFilePaths.get("agentList"), Agent.class);
		Agent agent1 = agents.stream().filter(
				agent -> agent.getFirstName().equals(agentFirstName) && agent.getLastName().equals(agentLastName))
				.findFirst().get();
		List<Customer> customers = readCsvFile(csvFilePaths.get("customerList"), Customer.class);
		return (short) customers.stream().filter(
				customer -> customer.getArea().equals(customerArea) && agent1.getAgentId() == customer.getAgentId())
				.collect(Collectors.toList()).size();
//		csvFilePaths.forEach((individual, filePath) -> {
//			if (individual.equals("agentList")) {
//				List<Agent> allAgents = readCsvFile(filePath, Agent.class);
//				agents.addAll(allAgents.stream().filter(agent -> agent.getFirstName().equals(agentFirstName)
//						&& agent.getLastName().equals(agentLastName)).collect(Collectors.toList()));
//				System.out.println("agent found:");
//				System.out.println(agents.get(0).getFirstName());
//			} else {
//				List<Customer> allCustomers = readCsvFile(filePath, Customer.class);
//				i.set(allCustomers.stream().filter(customer -> agents.get(0).getAgentId() == customer.getAgentId()).collect(Collectors.toList())
//						.size());
//			}
//		});

	}

	
	/* #5
	 * getCustomersRetainedForYearsByPlcyCostAsc() -- Return a list of customers retained for a given number of years, in ascending order of their policy cost.
	 * @param filePath -- Path to file being read in.
	 * @param yearsOfServeice -- Number of years the person has been a customer.
	 * @return -- List of customers retained for a given number of years, in ascending order of policy cost.
	 */
	public List<Customer> getCustomersRetainedForYearsByPlcyCostAsc(String customerFilePath, short yearsOfService) {
		List<Customer> customers = readCsvFile(customerFilePath, Customer.class);
		return customers.stream().filter(customer -> (short) customer.getYearsOfService() == yearsOfService)
				.sorted((cus1, cus2) -> cus1.getTotalMonthlyPremium().compareTo(cus2.getTotalMonthlyPremium()))
				.collect(Collectors.toList());
	}

	
	/* #6
	 * getLeadsForInsurance() -- Return a list of individuals who’ve made an inquiry for a policy but have not signed up.
	 * *HINT* -- Look for customers that currently have no policies with the insurance company.
	 * @param filePath -- Path to file being read in.
	 * @return -- List of customers who’ve made an inquiry for a policy but have not signed up.
	 */
	public List<Customer> getLeadsForInsurance(String filePath) {
		List<Customer> customers = readCsvFile(filePath, Customer.class);
		return customers.stream()
				.filter(customer -> !customer.isHomePolicy() && !customer.isAutoPolicy() && !customer.isRentersPolicy())
				.collect(Collectors.toList());
	}


	/* #7
	 * getVendorsWithGivenRatingThatAreInScope() -- Return a list of vendors within an area and include options to narrow it down by: 
			a.	Vendor rating
			b.	Whether that vendor is in scope of the insurance (if inScope == false, return all vendors in OR out of scope, if inScope == true, return ONLY vendors in scope)
	 * @param filePath -- Path to file being read in.
	 * @param area -- Area of the vendor.
	 * @param inScope -- Whether or not the vendor is in scope of the insurance.
	 * @param vendorRating -- The rating of the vendor.
	 * @return -- List of vendors within a given area, filtered by scope and vendor rating.
	 */
	public List<Vendor> getVendorsWithGivenRatingThatAreInScope(String filePath, String area, boolean inScope,
			int vendorRating) {
		List<Vendor> vendors = readCsvFile(filePath, Vendor.class);
		List<Vendor> vendors1 = vendors.stream().filter(vendor -> vendor.getArea().equals(area))
				.collect(Collectors.toList());
		List<Vendor> vendors2 = vendors1.stream()
				.filter(vendor ->  vendor.getVendorRating() == vendorRating)
				.collect(Collectors.toList());
		if (!inScope) {
			return vendors2;
		}
		return vendors2.stream().filter(vendor -> vendor.isInScope()).collect(Collectors.toList());
	}


	/* #8
	 * getUndisclosedDrivers() -- Return a list of customers between the age of 40 and 50 years (inclusive), who have:
			a.	More than X cars
			b.	less than or equal to X number of dependents.
	 * @param filePath -- Path to file being read in.
	 * @param vehiclesInsured -- The number of vehicles insured.
	 * @param dependents -- The number of dependents on the insurance policy.
	 * @return -- List of customers filtered by age, number of vehicles insured and the number of dependents.
	 */
	public List<Customer> getUndisclosedDrivers(String filePath, int vehiclesInsured, int dependents) {
		List<Customer> customers = readCsvFile(filePath, Customer.class);
		return customers.stream()
				.filter(customer -> customer.getAge() >= 40 && customer.getAge() <= 50
						&& customer.getVehiclesInsured() > vehiclesInsured
						&& customer.getDependents().size() <= dependents)
				.collect(Collectors.toList());
	}	


	/* #9
	 * getAgentIdGivenRank() -- Return the agent with the given rank based on average customer satisfaction rating. 
	 * *HINT* -- Rating is calculated by taking all the agent rating by customers (1-5 scale) and dividing by the total number 
	 * of reviews for the agent.
	 * @param filePath -- Path to file being read in.
	 * @param agentRank -- The rank of the agent being requested.
	 * @return -- Agent ID of agent with the given rank.
	 */
	public int getAgentIdGivenRank(String filePath, int agentRank) {
		List<Customer> customers = readCsvFile(filePath, Customer.class);
		Map<Integer, Double> agentWithRating = customers.stream().collect(
				Collectors.groupingBy(Customer::getAgentId, Collectors.averagingInt(Customer::getAgentRating)));
		List<Entry<Integer, Double>> list = new ArrayList<>(agentWithRating.entrySet());
		list.sort(Entry.comparingByValue());
		Collections.reverse(list);
		System.out.println(list);
		return list.get(agentRank - 1).getKey();
	}	

	
	/* #10
	 * getCustomersWithClaims() -- Return a list of customers who’ve filed a claim within the last <numberOfMonths> (inclusive). 
	 * @param filePath -- Path to file being read in.
	 * @param monthsOpen -- Number of months a policy has been open.
	 * @return -- List of customers who’ve filed a claim within the last <numberOfMonths>.
	 */
	public List<Customer> getCustomersWithClaims(Map<String, String> csvFilePaths, short monthsOpen) {
		List<Customer> customers = readCsvFile(csvFilePaths.get("customerList"), Customer.class);
		List<Claim> claims = readCsvFile(csvFilePaths.get("claimList"), Claim.class);
		Set<Integer> customerIds = claims.stream().filter(claim -> (short) claim.getMonthsOpen() <= monthsOpen)
				.map(Claim::getCustomerId).collect(Collectors.toSet());
		return customers.stream().filter(customer -> customerIds.contains(customer.getCustomerId()))
				.collect(Collectors.toList());
	}	

}
