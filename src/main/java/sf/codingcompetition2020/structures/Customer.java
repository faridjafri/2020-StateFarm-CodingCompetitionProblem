package sf.codingcompetition2020.structures;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Customer {
	private int customerId;
	private String firstName;
	private String lastName;
	private int age;
	private String area;
	private int agentId;
	private short agentRating;
	private String primaryLanguage;
	private List<Dependent> dependents;
	private boolean homePolicy;
	private boolean autoPolicy;
	private boolean rentersPolicy;
	private String totalMonthlyPremium;
	private short yearsOfService;
	private Integer vehiclesInsured;

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public int getAgentId() {
		return agentId;
	}

	public void setAgentId(int agentId) {
		this.agentId = agentId;
	}

	public short getAgentRating() {
		return agentRating;
	}

	public void setAgentRating(short agentRating) {
		this.agentRating = agentRating;
	}

	public String getPrimaryLanguage() {
		return primaryLanguage;
	}

	public void setPrimaryLanguage(String primaryLanguage) {
		this.primaryLanguage = primaryLanguage;
	}

	public List<Dependent> getDependents() {
		return dependents;
	}

	public void setDependents(String dependents) {
		try {
			this.dependents = new ObjectMapper().configure(Feature.AUTO_CLOSE_SOURCE, true).readValue(dependents,
					new TypeReference<List<Dependent>>() {
					});
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			this.dependents = new ArrayList<>();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			this.dependents = new ArrayList<>();
		}
	}

	public boolean isHomePolicy() {
		return homePolicy;
	}

	public void setHomePolicy(boolean homePolicy) {
		this.homePolicy = homePolicy;
	}

	public boolean isAutoPolicy() {
		return autoPolicy;
	}

	public void setAutoPolicy(boolean autoPolicy) {
		this.autoPolicy = autoPolicy;
	}

	public boolean isRentersPolicy() {
		return rentersPolicy;
	}

	public void setRentersPolicy(boolean rentersPolicy) {
		this.rentersPolicy = rentersPolicy;
	}

	public String getTotalMonthlyPremium() {
		return totalMonthlyPremium;
	}

	public void setTotalMonthlyPremium(String totalMonthlyPremium) {
		this.totalMonthlyPremium = totalMonthlyPremium;
	}

	public short getYearsOfService() {
		return yearsOfService;
	}

	public void setYearsOfService(short yearsOfService) {
		this.yearsOfService = yearsOfService;
	}

	public Integer getVehiclesInsured() {
		return vehiclesInsured;
	}

	public void setVehiclesInsured(Integer vehiclesInsured) {
		this.vehiclesInsured = vehiclesInsured;
	}

}
