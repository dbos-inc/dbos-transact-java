package dev.dbos.transact.json;

import java.util.Objects;


public class Person {
    public String name;
    public int age;
    public Address address;

    public Person() {}

    public Person(String name, int age, Address address) {
        this.name = name;
        this.age = age;
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Person)) return false;
        Person other = (Person) o;
        return Objects.equals(name, other.name)
                && age == other.age
                && Objects.equals(address, other.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age, address);
    }

    public static class Address {
        public String city;
        public String zip;

        public Address() {} // Required for Jackson

        public Address(String city, String zip) {
            this.city = city;
            this.zip = zip;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Address)) return false;
            Address other = (Address) o;
            return Objects.equals(city, other.city) && Objects.equals(zip, other.zip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(city, zip);
        }
    }
}

