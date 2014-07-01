/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.indigo;

import java.util.logging.Logger;

/**
 * Class that maintains data-center constants
 * 
 * @author smduarte
 * 
 */
public class Defaults {
	public static final Logger DCLogger = Logger.getLogger("swift.indigo");

	public static final String REMOTE_INDIGO_URL = "tcp://*:30007";

}
