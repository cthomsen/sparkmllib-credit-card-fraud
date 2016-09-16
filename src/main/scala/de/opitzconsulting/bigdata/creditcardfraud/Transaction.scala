package de.opitzconsulting.bigdata.creditcardfraud

import java.sql.Date

case class Transaction(customerId: String, partnerId: String, location: String, date: Date, amount: Float, fraud: Boolean)
