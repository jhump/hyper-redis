package com.bluegosling.redis.values;

/**
 * The types of aggregation supported for combining scores when computing intersections and unions
 * of sorted sets.
 * 
 * @author Joshua Humphries (jhumphries131@gmail.com)
 */
public enum AggregateType {
   SUM, MIN, MAX
}
