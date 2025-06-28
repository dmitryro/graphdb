use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MasterPatientIndex {
    pub id: i32,
    pub patient_id: Option<i32>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub date_of_birth: Option<DateTime<Utc>>,
    pub gender: Option<String>,
    pub address: Option<String>,
    pub contact_number: Option<String>,
    pub email: Option<String>,
    pub social_security_number: Option<String>,
    pub match_score: Option<f32>,
    pub match_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MasterPatientIndex {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MasterPatientIndex").expect("Invalid Identifier");
        let mut v = Vertex::new(id_type);

        v.add_property("id", &self.id.to_string());

        if let Some(ref val) = self.patient_id {
            v.add_property("patient_id", &val.to_string());
        }
        if let Some(ref val) = self.first_name {
            v.add_property("first_name", val);
        }
        if let Some(ref val) = self.last_name {
            v.add_property("last_name", val);
        }
        if let Some(ref val) = self.date_of_birth {
            v.add_property("date_of_birth", &val.to_rfc3339());
        }
        if let Some(ref val) = self.gender {
            v.add_property("gender", val);
        }
        if let Some(ref val) = self.address {
            v.add_property("address", val);
        }
        if let Some(ref val) = self.contact_number {
            v.add_property("contact_number", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        if let Some(ref val) = self.social_security_number {
            v.add_property("ssn", val);
        }
        if let Some(ref val) = self.match_score {
            v.add_property("match_score", &val.to_string());
        }
        if let Some(ref val) = self.match_date {
            v.add_property("match_date", &val.to_rfc3339());
        }

        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());

        v
    }
}

