package com.beta.model

import com.fasterxml.jackson.annotation.JsonProperty

case class Sc(@JsonProperty(value = "OrderDate")   orderDate: String,
              @JsonProperty("Region") region: String,
              @JsonProperty(value = "Rep1",required = false ,defaultValue ="ttt" ) rep: String,
              @JsonProperty("Item") item: String,
              @JsonProperty("Units") units: String,
              @JsonProperty("Unit Cost") unitsCost: Float,
              @JsonProperty("Total") total: Float
             )
