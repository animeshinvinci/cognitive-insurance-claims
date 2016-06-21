package com.ibm.cicto.helpers
case class ClaimComplete(
  approvedAmount: Long,
  vehicleType: String,
  claimId: String,
  yearOfVehicle: Long,
  homeState: String,
  creditScore: Long,
  estimate: Long,
  model: String,
  make: String,
  claimType: String,
  approved: String)

