using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.Net;
using System.Net.Mail;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Web;
using System.Web.Security;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Collections.Specialized;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace moodlerest
{
    public class user
    {
        // Object to hold user data.

        public string firstname { get; set; }
        public string middlename { get; set; }
        public string lastname { get; set; }
        private string _email;
        public string alternatename { get; set; }
        public string city { get; set; }
        public string country { get; set; }
        public string candidate_number { get; set; }
        public string sapcustomernumber { get; set; }
        public string sapsuppliernumber { get; set; }
        public string app_number { get; set; }
        public string password { get; set; }
        public bool generate_password { get; set; }
        public bool force_password_change { get; set; }
        public string institution { get; set; }
        public string address { get; set; }
        public string phone1 { get; set; }
        public string phone2 { get; set; }
        public string url { get; set; }

        public string email
        {
            get 
            {
                return _email.ToString();
            }
            set
            {
                _email = value.ToLower();
            }
        }

        public bool Equals(user p)
        {
            // If parameter is null return false;
            if((object) p == null)
            {
                return false;
            }

            // return true if the fields match
            bool match = true;

            foreach(PropertyInfo property in typeof(user).GetProperties())
            {
                if (property.GetValue(this) != property.GetValue(p))
                {
                    match = false;
                    break;
                }
            }

            return match;
        }
    }


    public class lkcriteria
    {
        // Object to hold criteria for looking up users.

        public string key { get; set; }
        public string value { get; set; }
    }

    public class lkupidnumber
    {
        // Object to hold full lookup users request.

        public string wsfunction { get; set; }
        public List<lkcriteria> criteria { get; set; }

        public lkupidnumber()
        {
            wsfunction = "local_extrauserlookups_get_users";
            criteria = new List<lkcriteria>();
        }
    }

    public class CheckForUser
    {
        // Class to check if a user already exists

        public Dictionary<user, JToken> duplicates { get; set; } // Dictionary of users that appear to be duplicates (including error JTokens)
        public Dictionary<user, String> existingusers { get; set; } // Dictionary of users that already exist (to lookup ids)
        public List<user> newusers { get; set; }  // List of users that are not currently on system (to be added)

        public CheckForUser()
        {
            // Initialise storage objects.

            duplicates = new Dictionary<user, JToken>();
            existingusers = new Dictionary<user, string>();
            newusers = new List<user>();
        }

        public async Task<string> ExistingCheck(OCWebRequest moodle, user checkuser) 
        {
            lkupidnumber userlookup;
            JObject jsonresponse;
            string id = "";
            JArray matchedusers = new JArray();
            JArray errors = new JArray();
            List<string> usernames = new List<string>();


            if( !String.IsNullOrWhiteSpace(checkuser.candidate_number) )
            {
                // Check to see if candidate number already exists as idnumber.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() { key = "idnumber", value = checkuser.candidate_number });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that id, so save it for later.
                    matchedusers = jsonresponse["users"] as JArray;
                }

                // Check to see if candidate number already exists.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() {key = "profile_field_candno", value = checkuser.candidate_number });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that candidate number, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union });
                }
            }

            if( !String.IsNullOrWhiteSpace(checkuser.sapcustomernumber) )
            {
                // Check to see if sap customer number already exists as idnumber.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() { key = "idnumber", value = checkuser.sapcustomernumber });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that ID, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union });
                }

                // Check to see if sap customer number already exists.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() {key = "profile_field_sapc", value = checkuser.sapcustomernumber });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with the SAP Customer number, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union });
                }
            }

            if( !String.IsNullOrWhiteSpace(checkuser.sapsuppliernumber) )
            {
                // Check to see if sap supplier number already exists as idnumber.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() { key = "idnumber", value = checkuser.sapsuppliernumber });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that ID, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union });
                }

                // Check to see if sap supplier number already exists.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() {key = "profile_field_saps", value = checkuser.sapsuppliernumber });

                await moodle.Send(userlookup);
                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);
                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that SAP Supplier number, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union });
                }
            }

            // Set merge settings so we can merge multiple errors.
            JsonMergeSettings mergeset = new JsonMergeSettings() { MergeArrayHandling = MergeArrayHandling.Union };

            if (!String.IsNullOrWhiteSpace(checkuser.email))
            {
                // Check to see if email already exists.
                userlookup = new lkupidnumber();
                userlookup.criteria.Add(new lkcriteria() { key = "email", value = checkuser.email });

                await moodle.Send(userlookup);

                jsonresponse = new JObject();

                jsonresponse = JObject.Parse(moodle.response);

                if (jsonresponse["users"] != null && jsonresponse["users"].HasValues)
                {
                    // There's a user with that email, so save it for later.
                    matchedusers.Merge(jsonresponse["users"], mergeset);
                }
            }

            if( matchedusers.HasValues )
            {
                // There are existing users that match the current user, so check to see if they're the same person.

                bool dup = false;
                foreach( JToken match in matchedusers.Children())
                {
                    bool cfieldmatch = false;

                    if( match["customfields"] != null && match["customfields"].HasValues )
                    {
                        // Check custom fields to check for clashes.
                        List<string> candnos = new List<string>();
                        List<string> sapcs = new List<string>();
                        List<string> sapss = new List<string>();

                        foreach(JToken cfield in match["customfields"].Children()) {
                            if (cfield["shortname"].ToString() == "candno" && !String.IsNullOrWhiteSpace(checkuser.candidate_number))
                            {
                                if(checkuser.candidate_number != cfield["value"].ToString())
                                {
                                    // Candidate number mismatch!  We have a duplicate.
                                    dup = true;
                                    candnos.Add(cfield["value"].ToString());
                                    break;
                                }
                                else
                                {
                                    // Candidate number exists and matches, so note that we have an ID match.
                                    cfieldmatch = true;
                                }
                            }
                            if (cfield["shortname"].ToString() == "sapc" && !String.IsNullOrWhiteSpace(checkuser.sapcustomernumber))
                            {
                                if(checkuser.sapcustomernumber != cfield["value"].ToString())
                                {
                                    // SAP Customer mismatch! We have a duplicate.
                                    dup = true;
                                    sapcs.Add(cfield["value"].ToString());
                                    break;                            
                                }
                                else
                                {
                                    // SAP Customer exists and matches, so note that we have an ID match.
                                    cfieldmatch = true;
                                }
                            }
                            if (cfield["shortname"].ToString() == "saps" && !String.IsNullOrWhiteSpace(checkuser.sapsuppliernumber))
                            {
                                if(checkuser.sapsuppliernumber != cfield["value"].ToString())
                                {
                                    // SAP Supplier mismatch! We have a duplicate.
                                    dup = true;
                                    sapss.Add(cfield["value"].ToString());
                                    break;
                                }
                                else
                                {
                                    // SAP Supplier exists and matches, so note that we have an ID match.
                                    cfieldmatch = true;
                                }
                            }
                        }
                        if(candnos.Count > 0)
                        {
                            // We have candidate number mismatches, so raise an error.
                            candnos.Add(checkuser.candidate_number);
                            errors.Add(JObject.FromObject(new Dictionary<String, JToken>() {{"error", JToken.FromObject("Candidate Number mismatch(s)")}, {"Candidate Nos", JToken.FromObject(candnos)}}));
                        }
                        if(sapcs.Count > 0)
                        {
                            // We have SAP Customer mismatches, so raise an error.
                            sapcs.Add(checkuser.sapcustomernumber);
                            errors.Add(JObject.FromObject(new Dictionary<String, JToken>() {{"error", JToken.FromObject("SAP Customer Number mismatch(s)")}, {"SAP Nos", JToken.FromObject(sapcs)}}));
                        }
                        if(sapss.Count > 0)
                        {
                            // We have SAP Supplier mismatches, so raise an error.
                            sapss.Add(checkuser.sapsuppliernumber);
                            errors.Add(JObject.FromObject(new Dictionary<String, JToken>() {{"error", JToken.FromObject("SAP Supplier Number mismatch(s)")}, {"SAP Nos", JToken.FromObject(sapss)}}));
                        }
                    }

                    if( cfieldmatch == false )
                    {
                        // no corresponding cfields, so must match by idnumber or email only.  If so treat as duplicate for manual checking.
                        dup = true;
                        errors.Add(JObject.FromObject(new Dictionary<string, String> () {{"error","User with matching email address but no Candidate or SAP numbers to verify"},{"user",match["username"].ToString()}}));
                    }

                    if (!usernames.Contains(match["username"].ToString()))
                    {
                        // Note all matching usernames, so we can see how many matches we have.
                        usernames.Add(match["username"].ToString());
                    }
                }
                
                if(usernames.Count == 0)
                {
                    // Something has gone wrong.  Report as duplicate for investigation;

                    errors.Add(JObject.FromObject(new Dictionary<string, string>() {{"error","Match found, but couldn't retrieve username"}}));
                    dup = true;
                }
                else if(usernames.Count == 1)
                {
                    // Exactly one matching username found.

                    if( dup == false ) {
                        // No other issues found.
                        if (matchedusers.First()["id"] != null)
                        {
                            // Add as an existing user.
                            id = matchedusers.First()["id"].ToString();
                            existingusers.Add(checkuser, id);
                        }
                        else
                        {
                            // Can't get ID from JSON string, so return as duplicate for further investigation.
                            dup = true;
                            errors.Add(JObject.FromObject(new Dictionary<string, string>() {{ "error", "Cannot retrieve ID of user"}}));
                        }
                    }
                }
                else
                {
                     // Multiple matching usernames found.  Return as duplicates for further investigation.

                     errors.Add(JObject.FromObject(new Dictionary<string, JToken>() { { "error", JToken.FromObject("Multiple matching users") }, { "users", JToken.FromObject(usernames) } }));
                     dup = true;
                }

                if( dup )
                {
                    // We have duplicates, add to duplicates object.

                    JObject duprets = new JObject();
                    duprets["errors"] = errors;
                    JObject checkuserjson = JObject.FromObject(checkuser);
                    checkuserjson.Property("password").Remove();
                    duprets["user to add"] = checkuserjson;
                    duprets["matching existing users"] = matchedusers;
                    duplicates.Add(checkuser, duprets);
                }
            }
            else
            {
                // No matches, so treat as new user.
                newusers.Add(checkuser);
            }

            // Return the id if we were able to find a match, otherwise nothing if duplicate or new user.
            return id;
        }
    }

    public static class GetUsers
    {
        // Get user as dataset from ODBC or SQL source.

        public static DataTable ODBC(string connectionstring, string query) 
        {
            // Setup connection using provided string.
            OdbcConnection connection = new OdbcConnection();
            connection.ConnectionString = connectionstring;

            DataTable data = new DataTable();

            short retries = 0;

            OdbcDataAdapter adapter;

            // Try to get query (3 attempts).
            while (true)
            {
                try
                {
                    adapter = new OdbcDataAdapter(query, connection);
                    break;
                }
                catch(Exception)
                {
                    retries++;
                    if (retries > 2) throw;
                }
            }

            // Pull data into dataset.
            adapter.Fill(data);

            return data;
        }

        public static DataTable SQL(string connectionstring, string query)
        {
            // Setup connection using provided string.
            SqlConnection connection = new SqlConnection(connectionstring);

            DataTable data = new DataTable();

            short retries = 0;

            SqlDataAdapter adapter;

            // Try to get query (3 attempts).
            while (true)
            {
                try
                {
                    adapter = new SqlDataAdapter(query, connection);
                    break;
                }
                catch(Exception)
                {
                    retries++;
                    if (retries > 2) throw;
                }
            }

            // Pull data into dataset.
            adapter.Fill(data);

            return data;
        }

    }

    public static class ProcessData
    {
        public static List<Enrolment> Enrolments(DataTable data, Dictionary<string,string> fieldmappings)
        {
            List<Enrolment> Enrols = new List<Enrolment>();
            Dictionary<user, Dictionary<string, bool>> cohorts = new Dictionary<user, Dictionary<string,bool>>();
            Dictionary<user, Dictionary<string, courseenrol>> courses = new Dictionary<user, Dictionary<string, courseenrol>>();

            foreach (DataRow row in data.Rows)
            {
                // Cycle through all data;
                user newuser = new user();

                foreach (PropertyInfo propertyinfo in newuser.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    // Check which user fields exist in data and transfer to user object.

                    if (fieldmappings.ContainsKey(propertyinfo.Name))
                    {
                        if (data.Columns.Contains(fieldmappings[propertyinfo.Name]))
                        {
                            if(propertyinfo.GetType() == typeof(string))
                            {
                                propertyinfo.SetValue(newuser, row[fieldmappings[propertyinfo.Name]].ToString());
                            }
                            else if (propertyinfo.GetType() == typeof(bool))
                            {
                                bool value = true; // default to true (generate password, force change, etc)
                                bool.TryParse(row[fieldmappings[propertyinfo.Name]].ToString(), out value);
                                propertyinfo.SetValue(newuser, value);
                            }
                        }
                    }
                    else if (data.Columns.Contains(propertyinfo.Name))
                    {
                        if(propertyinfo.GetType() == typeof(string))
                        {
                            propertyinfo.SetValue(newuser, row[propertyinfo.Name].ToString());
                        }
                        else if (propertyinfo.GetType() == typeof(bool))
                        {
                            bool value = true; // default to true (generate password, force change, etc)
                            bool.TryParse(row[propertyinfo.Name].ToString(), out value);
                            propertyinfo.SetValue(newuser, value);
                        }
                    }
                }

                // Check for cohort enrolments
                string newcohort = "";
                bool newchdelete = false; // default to false (not deleted)

                if (fieldmappings.ContainsKey("cohort"))
                {
                    if(data.Columns.Contains(fieldmappings["cohort"]))
                    {
                        newcohort = row[fieldmappings["cohort"]].ToString();
                    }
                }
                else
                {
                    if(data.Columns.Contains("cohort"))
                    {
                        newcohort = row["cohort"].ToString();
                    }
                }

                if (fieldmappings.ContainsKey("chdelete"))
                {
                    if(data.Columns.Contains(fieldmappings["chdelete"]))
                    {
                        bool.TryParse(row[fieldmappings["chdelete"]].ToString(), out newchdelete);
                    }
                }
                else
                {
                    if(data.Columns.Contains("chdelete"))
                    {
                        bool.TryParse(row["chdelete"].ToString(), out newchdelete);
                    }
                }

                if(!string.IsNullOrWhiteSpace(newcohort))
                {
                    if(cohorts.ContainsKey(newuser))
                    {
                        // User already has cohorts.
                    
                        if(cohorts[newuser].ContainsKey(newcohort))
                        {
                            // User already has a record for this cohort.
                            if(cohorts[newuser][newcohort])
                            {
                                // Current record is marked as deleted, so replace with this one.
                                cohorts[newuser][newcohort] = newchdelete;
                            }
                        }
                        else {
                            // User doesn't have current record, so add it.
                            cohorts[newuser].Add(newcohort, newchdelete);
                        }
                    }
                }
                

                // Check for course enrolments.
                string newcourse = "";
                int newrole = 5;  // default role to 5 (student)
                bool newrdelete = false; // not deleted by default
                
                if (fieldmappings.ContainsKey("course")) 
                {
                    if(data.Columns.Contains(fieldmappings["course"]))
                    {
                        newcourse = row[fieldmappings["course"]].ToString();
                    }
                }
                else if(data.Columns.Contains("course")) {
                    newcourse = row["course"].ToString();
                }

                if (fieldmappings.ContainsKey("role")) 
                {
                    if(data.Columns.Contains(fieldmappings["role"]))
                    {
                        int.TryParse(row[fieldmappings["role"]].ToString(),out newrole);
                    }
                }
                else if(data.Columns.Contains("role"))
                {
                    int.TryParse(row["role"].ToString(), out newrole);
                }

                if (fieldmappings.ContainsKey("rdelete"))
                {
                    if(data.Columns.Contains(fieldmappings["rdelete"]))
                    {
                        bool.TryParse(row[fieldmappings["rdelete"]].ToString(), out newrdelete);
                    }
                }
                else if(data.Columns.Contains("rdelete"))
                {
                    bool.TryParse(row["rdelete"].ToString(), out newrdelete);
                }

                if(!string.IsNullOrWhiteSpace(newcourse))
                {
                    if(courses.ContainsKey(newuser)) {
                        // user already has courses to add.

                        if(courses[newuser].ContainsKey(newcourse)) {
                            // user already has roles in this course to add.

                            if(courses[newuser][newcourse].roles.ContainsKey(newrole.ToString())) {
                                // user already has this role in this course.
                                if(courses[newuser][newcourse].roles[newrole.ToString()]) {
                                    // if current value is deleted, replace with this one, otherwise leave as is
                                    courses[newuser][newcourse].roles[newrole.ToString()] = newrdelete;
                                }
                            }
                            else {
                                // user doesn't have this role, so add it
                                courses[newuser][newcourse].roles.Add(newrole.ToString(), newrdelete);
                            }
                        }
                        else {
                            // user doesn't have this course, so add it.
                            courseenrol myenrol = new courseenrol();
 
                            myenrol.roles.Add(newrole.ToString(), newrdelete);

                            courses[newuser].Add(newcourse, myenrol);
                        }
                    }
                    else {
                        // user doesn't currently have any courses, so add them.
                        courseenrol myenrol = new courseenrol();

                        myenrol.roles.Add(newrole.ToString(), newrdelete);

                        courses.Add(newuser, new Dictionary<string,courseenrol> {{newcourse, myenrol}});
                    }
                }
            
            }

            List<user> newusers = courses.Keys.ToList();
            newusers.AddRange(cohorts.Keys.Where(p2 => newusers.All(p1 => !p1.Equals(p2))).ToList());
            

            // construct Enrolment object
            foreach( user thisuser in newusers) 
            {
                Enrols.Add(new Enrolment {user = thisuser});

                if(courses.ContainsKey(thisuser)) {
                    Enrols.Last().courses = courses[thisuser];
                }

                if(cohorts.ContainsKey(thisuser)) {
                    Enrols.Last().cohorts = cohorts[thisuser];
                }
            }

            return Enrols;
        }
    }

    public class CreateUsers
    {
        // Class for creating new users.

        private string wsfunction { get; set; }
        public List<JToken> users { get; set; }
        public Dictionary<user, string> errors { get; set; }
        public string geouser { get; set; }
        private string _geopass;
        public OCWebRequest oc { get; set; }

        public string geopass
        {
            set
            {
                _geopass = value;
            }
        }

        public CreateUsers()
        {
            wsfunction = "local_extrauserlookups_create_users";
            users = new List<JToken>();
            errors = new Dictionary<user, string>();
            oc = new OCWebRequest();
        }

        public async Task AddUser( user user )
        {
            // Add the user in the parameters to the list to add to moodle.

            // Generate new username based on first initial and lastname
            string usernameprefix = user.firstname[0] + user.lastname;
            string username = await Username.Generate(oc, usernameprefix);  

            // Add core user details to list.
            users.Add(JObject.FromObject(new Dictionary<string,string> {{"username", username}, {"firstname", user.firstname}, {"lastname", user.lastname}, {"email", user.email}}));

            // Check if password has been specified and add if so.
            if(!string.IsNullOrWhiteSpace(user.password))
            {
                users.Last()["password"] = user.password;
            }
            else
            {
                // If no password, then pass random password string for now and generate a new password on the server instead.
                users.Last()["password"] = Membership.GeneratePassword(12, 3);
                user.generate_password = true;
            }

            // Check for other fields and add to object if necessary.
            if (!string.IsNullOrWhiteSpace(user.alternatename))
            {
                users.Last()["alternatename"] = user.alternatename;
            }
            if (!string.IsNullOrWhiteSpace(user.city))
            {
                users.Last()["city"] = user.city;
            }
            if(!string.IsNullOrWhiteSpace(user.country))
            {
                users.Last()["country"] = user.country;
            }
            if(!string.IsNullOrWhiteSpace(user.middlename))
            {
                users.Last()["middlename"] = user.middlename;
            }
            if(!string.IsNullOrWhiteSpace(user.institution))
            {
                users.Last()["institution"] = user.institution;
            }
            if(!string.IsNullOrWhiteSpace(user.address))
            {
                users.Last()["address"] = user.address;
            }
            if(!string.IsNullOrWhiteSpace(user.phone1))
            {
                users.Last()["phone1"] = user.phone1;
            }
            if(!string.IsNullOrWhiteSpace(user.phone2))
            {
                users.Last()["phone2"] = user.phone2;
            }
            if(!string.IsNullOrWhiteSpace(user.url))
            {
                users.Last()["url"] = user.url;
            }

            // Setup preferences;
            JArray preferences = new JArray();
            if (user.generate_password)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "create_password" }, { "value", "1" } }));
            }
            if (user.force_password_change)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "auth_forcepasswordchange" }, { "value", "1" } }));
            }
            if(preferences.HasValues) {
                users.Last()["preferences"] = preferences;
            }
            
            // Setup custom fields
            JArray customfields = new JArray();
            if( !String.IsNullOrWhiteSpace(user.candidate_number) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "candno" }, { "value", user.candidate_number } }));
                users.Last()["idnumber"] = user.candidate_number;
            }
            if ( !String.IsNullOrWhiteSpace(user.sapcustomernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string,string>{{"type", "SAPC"},{"value",user.sapcustomernumber}}));
                if( users.Last()["idnumber"] == null )
                {
                    users.Last()["idnumber"] = user.sapcustomernumber;
                }
            }
            if ( !String.IsNullOrWhiteSpace(user.sapsuppliernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "SAPS" }, { "value", user.sapsuppliernumber } }));
                if (users.Last()["idnumber"] == null)
                {
                    users.Last()["idnumber"] = user.sapsuppliernumber;
                }
            }
            if (customfields.HasValues)
            {
                users.Last()["customfields"] = customfields;
            }

            // Lookup timezone based on users location.
            users.Last()["timezone"] = await timezone.Lookup(user.city, user.country, geouser, _geopass);
        }

        public async Task AddUser( List<user> users) 
        {
            // Add a list of users to the object to upload.

            foreach( user user in users )
            {
                // For each user add them to the list.
                await this.AddUser(user);
            };
        }

        public async Task Send()
        {
            // Send the upload object to Moodle.
            await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users) } });
        }

    }

    public class UpdateUsers
    {
        // Class for updating existing users.

        private string wsfunction;
        public List<JToken> users { get; set; }
        public Dictionary<user, string> errors { get; set; }
        public string geouser { get; set; }
        private string _geopass;
        public OCWebRequest oc { get; set; }

        public string geopass
        {
            set
            {
                _geopass = value;
            }
        }

        public UpdateUsers()
        {
            wsfunction = "local_extrauserlookups_update_users";
            users = new List<JToken>();
            errors = new Dictionary<user, string>();
            oc = new OCWebRequest();
        }

        public async Task AddUser( string id, user user, bool updtz = false )
        {
            // Add user's ID to upload object.
            users.Add(JObject.FromObject(new Dictionary<string,string> {{"id", id}}));


            // Add any non-empty values to the details to update.
            if(!string.IsNullOrWhiteSpace(user.password))
            {
                users.Last()["password"] = user.password;
            }
            if(!string.IsNullOrWhiteSpace(user.firstname))
            {
                users.Last()["firstname"] = user.firstname;
            }
            if(!string.IsNullOrWhiteSpace(user.lastname))
            {
                users.Last()["lastname"] = user.lastname;
            }
            if(!string.IsNullOrWhiteSpace(user.email))
            {
                users.Last()["email"] = user.email;
            }
            if (!string.IsNullOrWhiteSpace(user.alternatename))
            {
                users.Last()["alternatename"] = user.alternatename;
            }
            if (!string.IsNullOrWhiteSpace(user.city))
            {
                users.Last()["city"] = user.city;
            }
            if(!string.IsNullOrWhiteSpace(user.country))
            {
                users.Last()["country"] = user.country;
            }
            if(!string.IsNullOrWhiteSpace(user.middlename))
            {
                users.Last()["middlename"] = user.middlename;
            } 
            if (!string.IsNullOrWhiteSpace(user.institution))
            {
                users.Last()["institution"] = user.institution;
            }
            if (!string.IsNullOrWhiteSpace(user.address))
            {
                users.Last()["address"] = user.address;
            }
            if (!string.IsNullOrWhiteSpace(user.phone1))
            {
                users.Last()["phone1"] = user.phone1;
            }
            if (!string.IsNullOrWhiteSpace(user.phone2))
            {
                users.Last()["phone2"] = user.phone2;
            }
            if (!string.IsNullOrWhiteSpace(user.url))
            {
                users.Last()["url"] = user.url;
            }

            // Setup preferences;
            JArray preferences = new JArray();
            if (user.generate_password)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "create_password" }, { "value", "1" } }));
            }
            if (user.force_password_change)
            {
                preferences.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "auth_forcepasswordchange" }, { "value", "1" } }));
            }
            if(preferences.HasValues) {
                users.Last()["preferences"] = preferences;
            }
            
            // Setup custom fields
            JArray customfields = new JArray();
            if( !String.IsNullOrWhiteSpace(user.candidate_number) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "SAPC" }, { "value", user.candidate_number } }));
                users.Last()["idnumber"] = user.candidate_number;
            }
            if ( !String.IsNullOrWhiteSpace(user.sapcustomernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string,string>{{"type", "SAPC"},{"value",user.sapcustomernumber}}));
                if( users.Last()["idnumber"] == null )
                {
                    users.Last()["idnumber"] = user.sapcustomernumber;
                }
            }
            if ( !String.IsNullOrWhiteSpace(user.sapsuppliernumber) )
            {
                customfields.Add(JObject.FromObject(new Dictionary<string, string> { { "type", "SAPS" }, { "value", user.sapsuppliernumber } }));
                if (users.Last()["idnumber"] == null)
                {
                    users.Last()["idnumber"] = user.sapsuppliernumber;
                }
            }
            if (customfields.HasValues)
            {
                users.Last()["customfields"] = customfields;
            }
            
            // Lookup new timezone only if requested.
            if (updtz) {
                users.Last()["timezone"] = await timezone.Lookup(user.city, user.country, geouser, _geopass);
            }
        }

        public async Task AddUser( Dictionary<string, user> users, bool updtz = false) 
        {
            // Add multiple users to the updates to upload.

            foreach( KeyValuePair<string, user> userpair in users )
            {
                // Add each user one at a time.
                await this.AddUser(userpair.Key, userpair.Value, updtz);
            };
        }

        public async Task Send()
        {
            // Send updates to moodle.
            await oc.Send(new Dictionary<string, JToken>() { { "wsfunction", JToken.FromObject(wsfunction) }, { "users", JToken.FromObject(users) } });
        }
    }

    public class courseenrol
    {
        // Object for holding course enrolment data.
        public Dictionary<string,bool> roles { get; set; }

        public courseenrol()
        {
            roles = new Dictionary<string, bool>();
        }
    }

    public class Enrolment
    {
        // Object for holding all enrolment data for processing.
        public user user {get; set;}
        public Dictionary<string,courseenrol> courses {get; set;}
        public Dictionary<string,bool> cohorts {get; set;}

        public Enrolment()
        {
            courses = new Dictionary<string, courseenrol>();
            cohorts = new Dictionary<string, bool>();
        }
    }

    public class newcourseenrol
    {
        // Object for holding course enrolments during processing.
        public string userid { get; set; }
        public string courseid { get; set; }
        public string roleid { get; set; }
        public string suspend { get; set; }
    }

    public class newroles
    {
        // Object for holding role assignments during processing.
        public string userid { get; set; }
        public string roleid { get; set; }
        public string instanceid { get; set; }
        public string contextlevel { get; set; }

        public newroles()
        {
            contextlevel = "course";
        }
    }

    public class newcohortenrol
    {
        // Object for holding cohort enrolments during processing.
        public Dictionary<string, string> cohorttype { get; set; }
        public Dictionary<string, string> usertype { get; set; }

        public newcohortenrol()
        {
            cohorttype.Add("type", "id");
            usertype.Add("type", "id");
        }
    }

    public class EnrolUsers
    {
        // Class for enrolling users onto courses and cohorts.
        private string fngetcourse;
        private string fngetcohorts;
        private string fngetenrolments;
        private string fnsetenrolments;
        private string fngetchmembers;
        private string fnsetchmembers;
        private string fnrmchmembers;
        private string fnsetroles;
        private string fnrmroles;
        private CheckForUser usercheck;
        public OCWebRequest oc { get; set; }
        public string geouser { get; set; }
        public string geopass { get; set; }

        public Dictionary<user,List<newcourseenrol>> newusercourses {get; set;}
        public Dictionary<user, List<newcohortenrol>> newusercohorts { get; set; }
        public List<newcourseenrol> enrols {get; set;}
        public List<newcohortenrol> cenrols { get; set; }
        public List<newcohortenrol> cremoves { get; set; }
        public List<newroles> addroles {get; set;} 
        public List<newroles> rmroles {get; set;}
        public Dictionary<string, user> upduser { get; set; }
        private Dictionary<string, string> courses;
        private Dictionary<string, string> cohorts;
        private Dictionary<string, JArray> courseenrolments;
        private Dictionary<string, JArray> cohortmembers;

        private List<Dictionary<string, string>> enrolgetopts;

        public EnrolUsers()
        {
            usercheck = new CheckForUser();

            // Set required functions
            fngetcourse = "core_course_get_courses";

            fngetcohorts = "local_extrauserlookups_get_cohorts";

            fngetenrolments = "core_enrol_get_enrolled_users"; 
            fnsetenrolments = "enrol_manual_enrol_users";
            fngetchmembers = "core_cohort_get_cohort_members";
            fnsetchmembers = "core_cohort_add_cohort_members";
            fnrmchmembers = "core_cohort_delete_cohort_members";

            fnsetroles = "core_role_assign_roles";
            fnrmroles = "core_role_unassign_roles";

            enrols = new List<newcourseenrol>();
            newusercourses = new Dictionary<user,List<newcourseenrol>>();
            newusercohorts = new Dictionary<user, List<newcohortenrol>>();
            cenrols = new List<newcohortenrol>();
            cremoves = new List<newcohortenrol>();
            addroles = new List<newroles>();
            rmroles = new List<newroles>();

            upduser = new Dictionary<string,user>();

            courses = new Dictionary<string, string>();
            cohorts = new Dictionary<string, string>();
            courseenrolments = new Dictionary<string, JArray>();
            cohortmembers = new Dictionary<string, JArray>();

            enrolgetopts = new List<Dictionary<string, string>>() { new Dictionary<string, string>() { { "name", "onlyactive" }, { "value", "1" } } };
        }

        private async Task<bool> SendEnrolment ()
        {
            // Unassign old roles.
            if (rmroles.Count > 0)
            {
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmroles) }, { "unassignments", JToken.FromObject(rmroles) } });
            }

            // Update course enrolments
            if (enrols.Count > 0)
            {
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetenrolments) }, { "enrolments", JToken.FromObject(enrols) } });
            }

            // Assign new roles.
            if (addroles.Count > 0)
            {
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetroles) }, { "assignments", JToken.FromObject(addroles) } });
            }

            // Remove old cohort enrolments
            if (cremoves.Count > 0)
            {
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmchmembers) }, { "members", JToken.FromObject(cremoves) } });
            }

            // Add new cohort enrolments
            if (cenrols.Count > 0)
            {
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetchmembers) }, { "members", JToken.FromObject(cenrols) } });
            }

            return true;
        }

        private async Task<bool> Unenrolnoroll()
        {
            List<newcourseenrol> unenrols = new List<newcourseenrol>();
            List<newroles> exroles = new List<newroles>();

            foreach(string course in courses.Values)
            {
                if (course != "1")
                {
                    await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fngetenrolments) }, { "options", JToken.FromObject(enrolgetopts) }, { "courseid", JToken.FromObject(course) } });

                    JArray enrolments = JArray.Parse(oc.response);

                    foreach (JToken enrolment in enrolments)
                    {
                        if (enrolment["roles"] == null || enrolment["roles"].Count() == 0 )
                        {
                            // User has no roles in course.
                            if (enrolment["id"] != null)
                            {
                                unenrols.Add(new newcourseenrol() { userid = enrolment["id"].ToString(), courseid = course, suspend = "1", roleid = "5" });
                                exroles.Add(new newroles() { userid = enrolment["id"].ToString(), instanceid = course, roleid = "5" });
                            }
                        }
                    }
                }
            }

            if(unenrols.Count > 0)
            {
                // Suspend user enrolments.
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnsetenrolments) }, { "enrolments", JToken.FromObject(unenrols) } });
            }

            if(exroles.Count > 0)
            {
                // Remove extra roles just created.
                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fnrmroles) }, { "unassignments", JToken.FromObject(exroles) } });
            }

            return true;
        }

        private async Task<bool> Check (Enrolment enrolment)
        {
            // Check which users need creating and which course and cohort enrolments to upload
            bool courseexist = false;
            bool cohortexist = false;
            string id = "";

            if (enrolment.courses.Count > 0)
            {
                // Only run if we have course enrolments to check.

                if (courses.Count == 0)
                {
                    // pull list of courses if we don't already have them.
                    await oc.Send(new Dictionary<string, string> { { "wsfunction", fngetcourse } });
                    JArray jcourses = JArray.Parse(oc.response);
                    foreach (JToken course in jcourses)
                    {
                        if (course.HasValues && course["id"] != null && course["shortname"] != null)
                        {
                            courses.Add(course["shortname"].ToString(), course["id"].ToString());
                        }
                    }
                }

                foreach (string course in enrolment.courses.Keys)
                {
                    // Check to see if any of the users courses are relevant for upload.
                    if (courses.ContainsKey(course))
                    {
                        courseexist = true;
                        break;
                    }
                }
            }

            if (courseexist)
            {
                //  There are relevant courses, so check to see if the user exists.
                id = await usercheck.ExistingCheck(oc, enrolment.user);
                if (String.IsNullOrWhiteSpace(id))
                {
                    if (usercheck.newusers.Contains(enrolment.user))
                    {
                        // user needs creating, so put relevant enrolments in a holding object for now until this has been done.
                        List<newcourseenrol> userscourses = new List<newcourseenrol>();
                        foreach (string course in enrolment.courses.Keys)
                        {
                            if (courses.ContainsKey(course))
                            {
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] != true)
                                    { // ignore any suspended enrolments.
                                        userscourses.Add(new newcourseenrol() { courseid = courses[course], roleid = role });
                                    }
                                }
                            }
                        }

                        if (userscourses.Count > 0)
                        {
                            // Only add user if there are enrolments to add.

                            if (newusercourses.ContainsKey(enrolment.user))
                            {
                                newusercourses[enrolment.user].AddRange(userscourses);
                            }
                            else
                            {
                                newusercourses.Add(enrolment.user, userscourses);
                            }
                        }
                    }
                }
                else
                {
                    // user already exists, so we just need to add the enrolments to the list to upload.
                    foreach (string course in enrolment.courses.Keys)
                    {
                        if (courses.ContainsKey(course))
                        {
                            if (!courseenrolments.ContainsKey(course))
                            {
                                // if we haven't already looked up the enrolments for the course, do so.
                                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fngetenrolments) }, { "options", JToken.FromObject(enrolgetopts) }, { "courseid", JToken.FromObject(courses[course]) } });
                                courseenrolments.Add(course, JArray.Parse(oc.response));
                            }

                            // select user if they exist

                            JObject userenrol = courseenrolments[course].Children<JObject>().FirstOrDefault(x => x["id"] != null && x["id"].ToString() == id);

                            if (userenrol == null)
                            {
                                // user not enrolled in course, so add enrolments
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] == false)
                                    { // ignore any suspended enrolments.
                                        enrols.Add(new newcourseenrol() { userid = id, courseid = courses[course], roleid = role });

                                        if (!upduser.ContainsKey(id))
                                        {
                                            upduser.Add(id, enrolment.user);
                                        }
                                    }
                                }
                            }
                            else if (userenrol["roles"] == null)
                            {
                                // user on course, but doesn't have any roles, so add them.
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    if (enrolment.courses[course].roles[role] == false)
                                    { // ignore any suspended enrolments.
                                        addroles.Add(new newroles() { userid = id, instanceid = courses[course], roleid = role });

                                        if (!upduser.ContainsKey(id))
                                        {
                                            upduser.Add(id,enrolment.user);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                JToken roles = userenrol["roles"];
                                // user is on course with roles, so check if these need updating.
                                foreach (string role in enrolment.courses[course].roles.Keys)
                                {
                                    JToken userrole = roles.FirstOrDefault(x => x["roleid"] != null && x["roleid"].ToString() == role);
                                    if (enrolment.courses[course].roles[role] == false) // not suspended
                                    {
                                        if (userrole == null)
                                        {
                                            // user doesn't currently have role
                                            addroles.Add(new newroles() { userid = id, instanceid = courses[course], roleid = role });

                                            if (!upduser.ContainsKey(id))
                                            {
                                                upduser.Add(id, enrolment.user);
                                            }
                                        }
                                    }
                                    else // suspended
                                    {
                                        if (enrolment.courses[course].roles[role] == true) // suspended
                                        {
                                            if (userrole != null)
                                            {
                                                //user currently has the role
                                                rmroles.Add(new newroles() { userid = id, instanceid = courses[course], roleid = role });
                                            }
                                        }
                                    }
                                }

                                if (userenrol["roles"] == null)
                                {
                                    // All roles removed, so unenrol.

                                    enrols.Add(new newcourseenrol() { userid = id, courseid = courses[course], suspend = "1", roleid = "5" });
                                }
                            }
                        }
                    }

                }
            }

            if (enrolment.cohorts.Count > 0)
            {
                // Only run if we have cohort enrolments to check.

                if (cohorts.Count == 0)
                {
                    await oc.Send(new Dictionary<string, string> { { "wsfunction", fngetcohorts } });
                    JArray jcohorts = JArray.Parse(oc.response);
                    foreach (JToken cohort in jcohorts)
                    {
                        if (cohort.HasValues && cohort["id"] != null && cohort["idnumber"] != null)
                        {
                            cohorts.Add(cohort["idnumber"].ToString(), cohort["id"].ToString());
                        }
                    }
                }

                foreach (string cohort in enrolment.cohorts.Keys)
                {
                    // Check to see if any of the users courses are relevant for upload.
                    if (cohorts.ContainsKey(cohort) && enrolment.cohorts[cohort] == false)
                    {
                        cohortexist = true;
                        break;
                    }
                }
            }

            if (cohortexist)
            {
                // There are relevant cohorts, so see if we have already checked if this user exists, if not run check
                if (!courseexist)
                {
                    id = await usercheck.ExistingCheck(oc, enrolment.user);
                }

                if (String.IsNullOrWhiteSpace(id))
                {
                    if (usercheck.newusers.Contains(enrolment.user))
                    {
                        // user needs creating, so put relevant enrolments in a holding object for now until this has been done.
                        List<newcohortenrol> userscohorts = new List<newcohortenrol>();
                        foreach(string cohort in enrolment.cohorts.Keys)
                        {
                            if(cohorts.ContainsKey(cohort) && enrolment.cohorts[cohort] == false)
                            {
                                userscohorts.Add(new newcohortenrol());
                                userscohorts.Last().cohorttype.Add("value", cohorts[cohort]);
                            }
                        }

                        if (userscohorts.Count > 0)
                        {
                            if (newusercohorts.ContainsKey(enrolment.user))
                            {
                                newusercohorts[enrolment.user].AddRange(userscohorts);
                            }
                            else
                            {
                                newusercohorts.Add(enrolment.user, userscohorts);
                            }
                        }
                    }
                }
                else
                {
                    // user already exists, so check cohort membership.
                    foreach(string cohort in enrolment.cohorts.Keys)
                    {
                        if (cohorts.ContainsKey(cohort))
                        {
                            if (!cohortmembers.ContainsKey(cohort))
                            {
                                // if we haven't already looked up the membership for the cohort, do so.
                                await oc.Send(new Dictionary<string, JToken> { { "wsfunction", JToken.FromObject(fngetchmembers) }, { "cohortids", JToken.FromObject(new List<string> { cohorts[cohort] }) } });
                                //cohortmembers.Add(cohort, JArray.Parse(oc.response));
                                JObject jres = JObject.Parse(oc.response);
                                jres = jres.Children<JObject>().FirstOrDefault(x => x["cohortid"] != null && x["cohortid"].ToString() == id);
                                JArray cmembers = new JArray();
                                if (jres != null && jres["userids"] != null)
                                {
                                    cmembers.Merge(jres["userids"]);
                                }
                                cohortmembers.Add(cohort, cmembers);
                            }

                            // select user if they exist
                            JObject usermembership = cohortmembers[cohort].Children<JObject>().FirstOrDefault(x => x != null && x.ToString() == id);

                            if (usermembership == null && enrolment.cohorts[cohort] == false)
                            {
                                // Object null and cohort enrolment not deleted, so user not in cohort and needs adding.
                                cenrols.Add(new newcohortenrol());
                                cenrols.Last().cohorttype.Add("value", cohorts[cohort]);
                                cenrols.Last().usertype.Add("value", id);

                                if (!upduser.ContainsKey(id))
                                {
                                    upduser.Add(id,enrolment.user);
                                }
                            }
                            else if (usermembership != null && enrolment.cohorts[cohort] == true)
                            {
                                // Object not null and cohort enrolment deleted, so user is in cohort and needs removing.
                                cremoves.Add(new newcohortenrol());
                                cremoves.Last().cohorttype.Add("value", cohorts[cohort]);
                                cremoves.Last().usertype.Add("value", id);
                            }
                        }
                    }
                }
            }

            return true;
        }

        public async Task<bool> Sync (List<Enrolment> enrolments)
        {
            foreach (Enrolment enrolment in enrolments)
            {
                // Check which enrolments need creating.
                await this.Check(enrolment);
            }

            // Create new users.
            CreateUsers newusers = new CreateUsers();
            newusers.oc = oc;
            newusers.geouser = geouser;
            newusers.geopass = geopass;
            List<user> addusers = newusercourses.Keys.ToList();
            addusers.AddRange(newusercohorts.Keys.Where(p2 => addusers.All(p1 => !p1.Equals(p2))).ToList());

            if (addusers.Count > 0)
            {
                await newusers.AddUser(addusers);
                await newusers.Send();

                foreach (user newuser in addusers)
                {
                    // Get generated id for each new user
                    string id = await usercheck.ExistingCheck(oc, newuser);
                    if (!String.IsNullOrWhiteSpace(id))
                    {
                        if (newusercourses.ContainsKey(newuser))
                        {
                            // If there are courses for that user add them.
                            foreach (newcourseenrol course in newusercourses[newuser])
                            {
                                enrols.Add(course);
                                enrols.Last().userid = id;
                            }
                        }

                        if (newusercohorts.ContainsKey(newuser))
                        {
                            // If there are cohorts for that user add them.
                            foreach (newcohortenrol cohort in newusercohorts[newuser])
                            {
                                cenrols.Add(cohort);
                                cenrols.Last().usertype.Add("value", id);
                            }
                        }
                    }

                }
            }

            // Update existing users
            if (upduser.Count > 0)
            {
                UpdateUsers update = new UpdateUsers();
                update.oc = oc;
                update.geouser = geouser;
                update.geopass = geopass;
                await update.AddUser(upduser);
                await update.Send();
            }
            

            // Send enrolment updates.
            await this.SendEnrolment();

            // Unenrol users without roles.
            await this.Unenrolnoroll();

            return true;
        }
    }

    public class OCWebRequest {
        public HttpClient moodle { get; set; }
        public HttpResponseMessage httpresponse { get; set; }
        public string resource { get; set; }
        private UriBuilder resourceuri;
        private NameValueCollection querystring;
        public string response { get; set; }

        // Define private variables for username and password, and set defaults.
        private string _token;

        // Create write-only properties to allow access to set token.
        public string token
        {
            set
            {
                _token = value;
            }
        }

        public OCWebRequest()
        {
            moodle = new HttpClient();
            httpresponse = new HttpResponseMessage();
        }

        // Request() method runs the REST request returning the HttpStatusCode 
        public async Task<HttpStatusCode> Send<T>(T request)
        {
            // Build uri from base request and resource and add username and password to query string.
            resourceuri = new UriBuilder(moodle.BaseAddress);
            querystring = HttpUtility.ParseQueryString(resourceuri.Query);

            querystring["wstoken"] = _token;

            resourceuri.Query = querystring.ToString();

            // Send REST request to OTRS.
            httpresponse = await moodle.PostAsJsonAsync(resourceuri.Uri.ToString(), request);

            // Process response.
            response = await httpresponse.Content.ReadAsStringAsync();

            // Return the HTTP Status Code.
            return httpresponse.StatusCode;
        }
    }

    public static class Username
    {
        // Static class for generating usernames.
        public static async Task<String> Generate ( OCWebRequest moodle, string usernameprefix )
        {
            // generate username.
            lkupidnumber userlookup;
            Regex usernamergx;
            JToken jsonresponse;
            List<int> suffixes;
            int suffix;
            string newsuffix;
            string returnval;
 
            // Remove whitespace and convert usernameprefix to lowercase
            
            usernameprefix = usernameprefix.Trim().ToLower().Replace(" ", String.Empty);

            // Regular expression, eliminate all chars EXCEPT:
            // alphanum, dash (-), underscore (_) and period (.) characters.

            usernamergx = new Regex("[^-\\._a-z0-9]");
            usernameprefix = usernamergx.Replace(usernameprefix, String.Empty);

            // Lookup any users currently with that prefix.
            userlookup = new lkupidnumber();
            userlookup.criteria.Add(new lkcriteria() { key = "username", value = usernameprefix + "%" });

            await moodle.Send(userlookup);


            // Process response into JToken object.
            jsonresponse = JToken.Parse(moodle.response);
            jsonresponse = jsonresponse["users"];


            // Transfer suffixes from ends of usernames to new List.
            suffixes = new List<int>();

            if (jsonresponse.HasValues)
            {
                foreach(JToken user in jsonresponse.Children())
                {
                    if (int.TryParse(user["username"].ToString().Substring(usernameprefix.Length), out suffix)) 
                    {
                        suffixes.Add(suffix);
                    }
                }
            }

            // If there are suffixes, then add one to the largest.
            if (suffixes.Count() > 0)
            {
                // find next number
                newsuffix = (suffixes.Max() + 1).ToString("D2"); // new suffix string with required leading zeros as necessary.
                returnval = usernameprefix + newsuffix;
            }
            else
            {
                // else start at 01.
                returnval = usernameprefix + "01";
            }

            return returnval;
        }
    }

    public class geonames
    {
        // Class for looking up geonames data.
        public HttpClient geonamehttp { get; set; }
        public HttpResponseMessage httpresponse { get; set; }
        public string resource { get; set; }
        private UriBuilder resourceuri;
        private NameValueCollection querystring;
        public string response { get; set; }

        // Define private variables for username and password, and set defaults.
        public string username { get; set; }
        private string _password;

        // Create write-only properties to allow access to set token.
        public string password
        {
            set
            {
                _password = value;
            }
        }

        public geonames()
        {
            geonamehttp = new HttpClient();
            geonamehttp.BaseAddress = new Uri("http://api.geonames.org/");
            httpresponse = new HttpResponseMessage();
        }

        // Request() method runs the REST request returning the HttpStatusCode 
        public async Task<HttpStatusCode> Send(NameValueCollection request)
        {
            // Build uri from base request and resource and add username and password to query string.
            resourceuri = new UriBuilder(geonamehttp.BaseAddress+resource);
            querystring = HttpUtility.ParseQueryString(resourceuri.Query);

            querystring.Add(request);

            querystring["username"] = username;
            querystring["password"] = _password;

            resourceuri.Query = querystring.ToString();

            // Send REST request to OTRS.
            httpresponse = await geonamehttp.GetAsync(resourceuri.Uri);

            // Process response.
            response = await httpresponse.Content.ReadAsStringAsync();

            // Return the HTTP Status Code.
            return httpresponse.StatusCode;
        }
    }

    public static class timezone
    {
        // static class for looking up timezones.
        public static async Task<string> Lookup (string city, string country, string username, string password, double fuzzy = 0.8)
        {
            geonames gnsearch = new geonames();
            geonames tzlookup = new geonames();
            string returntimezone = "";

            // Setup geonames data.
            gnsearch.resource = "searchJSON";
            gnsearch.username = username;
            gnsearch.password = password;

            tzlookup.resource = "timezoneJSON";
            tzlookup.username = username;
            tzlookup.password = password;

            if (!String.IsNullOrWhiteSpace(city))
            {
                // If we have a city, try looking that up first.

                NameValueCollection search = new NameValueCollection();

                search["q"] = city;
                search["country"] = country;
                search["fuzzy"] = fuzzy.ToString();
                search["orderby"] = "relevance";

                await gnsearch.Send(search);

                JObject searchresults = JObject.Parse(gnsearch.response);

                int noresults;

                // Process results.
                if (searchresults["totalResultsCount"] != null && Int32.TryParse(searchresults["totalResultsCount"].ToString(), out noresults))
                {
                    if (noresults > 0 && searchresults["geonames"] != null)
                    {
                        // Check to see if there are results.

                        foreach (JToken place in searchresults["geonames"].Children())
                        {
                            if (place["lat"] != null && place["lng"] != null)
                            {
                                NameValueCollection timezoneloc = new NameValueCollection();

                                // Lookup timezone from lat/lng figures for current result from geonames search.

                                timezoneloc["lat"] = place["lat"].ToString();
                                timezoneloc["lng"] = place["lng"].ToString();

                                await tzlookup.Send(timezoneloc);

                                JObject timezonedata = JObject.Parse(tzlookup.response);

                                if (timezonedata["timezoneId"] != null)
                                {
                                    returntimezone = timezonedata["timezoneId"].ToString();
                                    break;  // If we've found a timezone stop.  Otherwise continue loop with next result.
                                }
                            }
                        }
                    }
                }
            }

            if(String.IsNullOrEmpty(returntimezone) && ! String.IsNullOrWhiteSpace(country))
            {
                // Couldn't find timezone for city, so try for country's capital instead.
                geonames capitallookup = new geonames();

                capitallookup.resource = "countryInfoJSON";
                capitallookup.username = username;
                capitallookup.password = password;

                NameValueCollection countryinfo = new NameValueCollection();

                countryinfo["country"] = country;

                await capitallookup.Send(countryinfo);

                JObject capitalresults = JObject.Parse(capitallookup.response);

                if(capitalresults["geonames"] != null && capitalresults["geonames"].HasValues && capitalresults["geonames"].First()["capital"] != null && capitalresults["geonames"].First()["capital"].ToString() != city) {
                    // Try again with countries capital if it's different to the location just searched.
                    returntimezone = await timezone.Lookup(capitalresults["geonames"].First()["capital"].ToString(),country,username,password);
                }
            }

            if(String.IsNullOrWhiteSpace(returntimezone))
            {
                // Still couldn't find a timezone, so default to London.
                returntimezone = "Europe/London";
            }

            return returntimezone;
        }
    }
}
